"""
Управление расписаниями
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta, date as date_type
from typing import Dict, List, Optional, Tuple
from config import (
    SCHEDULES_DIR, REQUESTS_DIR, QUEUE_DIR, DEFAULT_SCHEDULE_FILE, 
    DEFAULT_SCHEDULE, MAX_OFFICE_SEATS, DATA_DIR,
    USE_GOOGLE_SHEETS, USE_GOOGLE_SHEETS_FOR_WRITES, USE_GOOGLE_SHEETS_FOR_READS,
    SHEET_REQUESTS, SHEET_SCHEDULES, SHEET_QUEUE, SHEET_DEFAULT_SCHEDULE,
    USE_POSTGRESQL
)
import pytz
from config import TIMEZONE
from utils import get_header_start_idx, filter_empty_rows, ensure_header

# Настройка логирования
logger = logging.getLogger(__name__)


def _merge_request_created_at(a, b):
    """Ранний created_at при слиянии заявок одного сотрудника (например, из разных источников)."""
    if a is not None and b is not None:
        return min(a, b)
    return a if a is not None else b


def _request_sort_key_for_week(req: Dict) -> tuple:
    """Сортировка заявок: раньше created_at — раньше обрабатываются (в т.ч. при конкуренции за место)."""
    ca = req.get('created_at')
    if ca is None:
        return (datetime.max, req.get('employee_name') or '')
    if isinstance(ca, date_type) and not isinstance(ca, datetime):
        ca = datetime.combine(ca, datetime.min.time())
    return (ca, req.get('employee_name') or '')

# Импортируем Google Sheets Manager только если нужно
if USE_GOOGLE_SHEETS:
    try:
        from google_sheets_manager import GoogleSheetsManager
    except ImportError:
        GoogleSheetsManager = None
else:
    GoogleSheetsManager = None

# Импортируем функции для работы с PostgreSQL
if USE_POSTGRESQL:
    try:
        from database import (
            save_schedule_to_db, save_default_schedule_to_db, save_request_to_db,
            clear_requests_from_db, add_to_queue_db, remove_from_queue_db,
            load_schedule_from_db, load_default_schedule_from_db,
            load_requests_from_db, load_queue_from_db
        )
    except ImportError:
        save_schedule_to_db = None
        save_default_schedule_to_db = None
        save_request_to_db = None
        clear_requests_from_db = None
        add_to_queue_db = None
        remove_from_queue_db = None
        load_schedule_from_db = None
        load_default_schedule_from_db = None
        load_requests_from_db = None
        load_queue_from_db = None
else:
    save_schedule_to_db = None
    save_default_schedule_to_db = None
    save_request_to_db = None
    clear_requests_from_db = None
    add_to_queue_db = None
    remove_from_queue_db = None
    load_schedule_from_db = None
    load_default_schedule_from_db = None
    load_requests_from_db = None
    load_queue_from_db = None


def _get_pool():
    """Получить пул подключений PostgreSQL (динамический импорт)"""
    if not USE_POSTGRESQL:
        return None
    try:
        from database import _pool
        return _pool
    except ImportError:
        return None


class ScheduleManager:
    """Класс для управления расписаниями"""
    
    def __init__(self, employee_manager=None):
        self.timezone = pytz.timezone(TIMEZONE)
        self.employee_manager = employee_manager
        
        # Инициализируем Google Sheets Manager если нужно
        self.sheets_manager = None
        if USE_GOOGLE_SHEETS and GoogleSheetsManager:
            try:
                self.sheets_manager = GoogleSheetsManager()
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Google Sheets для расписаний: {e}")
        
        self._ensure_directories()
        # Не сохраняем и не обновляем файлы - только PostgreSQL
    
    def _ensure_directories(self):
        """Создать необходимые директории"""
        os.makedirs(SCHEDULES_DIR, exist_ok=True)
        os.makedirs(REQUESTS_DIR, exist_ok=True)
        os.makedirs(QUEUE_DIR, exist_ok=True)
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def _save_default_schedule(self):
        """Сохранить расписание по умолчанию"""
        if os.path.exists(DEFAULT_SCHEDULE_FILE):
            return
        
        with open(DEFAULT_SCHEDULE_FILE, 'w', encoding='utf-8') as f:
            for day, employees in DEFAULT_SCHEDULE.items():
                f.write(f"{day}\n")
                f.write(f"{', '.join(employees)}\n")
    
    def load_default_schedule(self) -> Dict[str, Dict[str, str]]:
        """
        Загрузить расписание по умолчанию из PostgreSQL (приоритет), Google Sheets или файла
        Returns: Dict[str, Dict[str, str]] - {день: {место: имя}}
        """
        schedule = {}
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки при старте
        if USE_POSTGRESQL:
            try:
                from database_sync import load_default_schedule_from_db_sync
                logger.debug("Используем синхронную загрузку расписания по умолчанию из PostgreSQL")
                db_schedule = load_default_schedule_from_db_sync()
                logger.debug("load_default_schedule_from_db_sync завершен успешно")
                
                if db_schedule:
                    schedule = db_schedule
                    logger.info(f"Расписание по умолчанию загружено из PostgreSQL: {len(schedule)} дней")
                    return schedule
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания по умолчанию из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['day', 'day_name', 'День'])
                for row in rows[start_idx:]:
                    if len(row) >= 2:
                        try:
                            day_name = row[0].strip()
                            # Пытаемся распарсить как JSON
                            if row[1].strip().startswith('{'):
                                places_dict = json.loads(row[1].strip())
                                schedule[day_name] = places_dict
                            else:
                                # Старый формат (список через запятую) - конвертируем
                                employees_str = row[1].strip() if row[1] else ""
                                employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                                # Конвертируем в новый формат
                                places_dict = {}
                                for i, emp in enumerate(employees, 1):
                                    places_dict[f'1.{i}'] = emp
                                schedule[day_name] = places_dict
                        except (ValueError, IndexError, json.JSONDecodeError) as e:
                            logger.warning(f"Ошибка парсинга строки расписания: {e}")
                            continue
                # Если загрузили из Google Sheets, возвращаем результат
                if schedule:
                    return schedule
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания по умолчанию из Google Sheets: {e}, используем файлы")
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и PostgreSQL недоступен)
        if USE_GOOGLE_SHEETS_FOR_READS and not schedule and self.sheets_manager and self.sheets_manager.is_available():
            # Не проверяем буферизованные операции - работаем только с PostgreSQL
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['day', 'day_name', 'День'])
                for row in rows[start_idx:]:
                    if len(row) >= 2:
                        try:
                            day_name = row[0].strip()
                            # Пытаемся распарсить как JSON
                            if row[1].strip().startswith('{'):
                                places_dict = json.loads(row[1].strip())
                                schedule[day_name] = places_dict
                            else:
                                # Старый формат (список через запятую) - конвертируем
                                employees_str = row[1].strip() if row[1] else ""
                                employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                                # Конвертируем в новый формат
                                places_dict = {}
                                for i, emp in enumerate(employees, 1):
                                    places_dict[f'1.{i}'] = emp
                                schedule[day_name] = places_dict
                        except (ValueError, IndexError, json.JSONDecodeError) as e:
                            logger.warning(f"Ошибка парсинга строки расписания: {e}")
                            continue
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания по умолчанию из Google Sheets: {e}, используем config")
        
        # Если не загрузилось, используем из config
        if not schedule:
            schedule = DEFAULT_SCHEDULE.copy()
        
        return schedule
    
    def save_default_schedule(self, schedule: Dict[str, Dict[str, str]]):
        """
        Сохранить расписание по умолчанию в PostgreSQL, Google Sheets и файл (JSON формат)
        
        Args:
            schedule: Dict[str, Dict[str, str]] - расписание по дням, где внутренний словарь - места (ключ: "подразделение.место")
        """
        # Сохраняем в PostgreSQL (приоритет 1)
        if USE_POSTGRESQL:
            try:
                from database_sync import save_default_schedule_to_db_sync
                result = save_default_schedule_to_db_sync(schedule)
                if result:
                    logger.info("✅ Расписание по умолчанию сохранено в PostgreSQL")
                else:
                    logger.warning("⚠️ Расписание по умолчанию не сохранено в PostgreSQL (вернуло False)")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения расписания по умолчанию в PostgreSQL: {e}", exc_info=True)
        
        # Не сохраняем в файл - только PostgreSQL
    
    def get_plain_name_from_formatted(self, formatted_name: str) -> str:
        """Извлечь простое имя из отформатированного (например, 'Рома(@rsidorenkov)' -> 'Рома')"""
        if '(@' in formatted_name and formatted_name.endswith(')'):
            return formatted_name.split('(@')[0]
        return formatted_name
    
    def _default_schedule_to_list(self, schedule: Dict[str, Dict[str, str]]) -> Dict[str, List[str]]:
        """
        Конвертировать расписание по умолчанию из формата JSON в список для обратной совместимости
        
        Args:
            schedule: Dict[str, Dict[str, str]] - расписание в формате {день: {место: имя}}
            
        Returns:
            Dict[str, List[str]] - расписание в формате {день: [имена]}
        """
        result = {}
        for day_name, places_dict in schedule.items():
            # Сортируем места по номеру подразделения и месту
            sorted_places = sorted(places_dict.items(), key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
            result[day_name] = [name for _, name in sorted_places if name]
        return result
    
    def _list_to_default_schedule(self, schedule: Dict[str, List[str]], department: int = 1) -> Dict[str, Dict[str, str]]:
        """
        Конвертировать расписание по умолчанию из формата списка в JSON формат
        
        Args:
            schedule: Dict[str, List[str]] - расписание в формате {день: [имена]}
            department: int - номер подразделения (по умолчанию 1)
            
        Returns:
            Dict[str, Dict[str, str]] - расписание в формате {день: {место: имя}}
        """
        result = {}
        for day_name, employees in schedule.items():
            places_dict = {}
            for i, emp in enumerate(employees, 1):
                places_dict[f'{department}.{i}'] = emp
            result[day_name] = places_dict
        return result
    
    def _find_employee_in_places(self, places_dict: Dict[str, str], employee_name: str) -> Optional[str]:
        """
        Найти сотрудника в словаре мест и вернуть ключ места
        
        Args:
            places_dict: Dict[str, str] - словарь мест {место: имя}
            employee_name: str - имя сотрудника для поиска
            
        Returns:
            Optional[str] - ключ места (например, "1.1") или None
        """
        for place_key, name in places_dict.items():
            plain_name = self.get_plain_name_from_formatted(name)
            if plain_name == employee_name:
                return place_key
        return None
    
    def _get_employees_list_from_places(self, places_dict: Dict[str, str]) -> List[str]:
        """
        Получить список имен сотрудников из словаря мест (отсортированный по месту)
        Ограничивает количество до MAX_OFFICE_SEATS
        
        Args:
            places_dict: Dict[str, str] - словарь мест {место: имя}
            
        Returns:
            List[str] - список имен, отсортированный по номеру места (максимум MAX_OFFICE_SEATS)
        """
        sorted_places = sorted(places_dict.items(), key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
        employees = [name for _, name in sorted_places if name]
        # Ограничиваем до MAX_OFFICE_SEATS мест
        return employees[:MAX_OFFICE_SEATS]
    
    def _find_free_place(self, places_dict: Dict[str, str], department: int = 1) -> Optional[str]:
        """
        Найти свободное место в словаре мест
        
        Args:
            places_dict: Dict[str, str] - словарь мест {место: имя}
            department: int - номер подразделения
            
        Returns:
            Optional[str] - ключ свободного места (например, "1.1") или None
        """
        for i in range(1, MAX_OFFICE_SEATS + 1):
            place_key = f'{department}.{i}'
            if place_key not in places_dict or not places_dict[place_key]:
                return place_key
        return None
    
    def get_week_start(self, date: Optional[datetime] = None) -> datetime:
        """Получить начало недели (понедельник) для указанной даты"""
        if date is None:
            date = datetime.now(self.timezone)
        elif date.tzinfo is None:
            date = self.timezone.localize(date)
        
        # Понедельник = 0
        days_since_monday = date.weekday()
        week_start = date - timedelta(days=days_since_monday)
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    
    def get_week_dates(self, week_start: datetime) -> List[Tuple[datetime, str]]:
        """Получить даты рабочей недели (Пн-Пт)"""
        weekdays = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
        dates = []
        for i, day_name in enumerate(weekdays):
            date = week_start + timedelta(days=i)
            dates.append((date, day_name))
        return dates
    
    def has_saved_schedules_for_week(self, week_start: datetime) -> bool:
        """
        Проверить, есть ли сохраненные расписания для недели
        (проверяет PostgreSQL, локальные файлы и Google Sheets)
        
        Args:
            week_start: Начало недели
            
        Returns:
            True если есть сохраненные расписания, False иначе
        """
        week_dates = self.get_week_dates(week_start)
        week_dates_str = [d.strftime('%Y-%m-%d') for d, _ in week_dates]
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для проверки
        if USE_POSTGRESQL:
            try:
                from database_sync import load_schedule_from_db_sync
                for date_str in week_dates_str:
                    db_schedule = load_schedule_from_db_sync(date_str)
                    if db_schedule:
                        logger.debug(f"Найдено сохраненное расписание для недели {week_start.strftime('%Y-%m-%d')} в PostgreSQL")
                        return True
            except Exception as e:
                logger.warning(f"Ошибка проверки расписаний в PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Локальные файлы
        for d, day_name in week_dates:
            date_str = d.strftime('%Y-%m-%d')
            schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
            if os.path.exists(schedule_file):
                return True
        
        # ПРИОРИТЕТ 3: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен)
        # ВАЖНО: Проверяем наличие буферизованных операций - если есть, не проверяем Google Sheets
        # чтобы не перезаписать актуальные данные из локальных файлов
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_SCHEDULES)
            if has_buffered:
                logger.debug(f"Есть буферизованные операции для {SHEET_SCHEDULES}, пропускаем проверку Google Sheets")
                return False
            
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
                rows = filter_empty_rows(rows)
                if not rows:
                    return False
                    
                start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                for row in rows[start_idx:]:
                    if len(row) >= 1 and row[0] and row[0].strip() in week_dates_str:
                        logger.debug(f"Найдено сохраненное расписание для недели {week_start.strftime('%Y-%m-%d')} в Google Sheets")
                        return True
            except Exception as e:
                logger.warning(f"Ошибка проверки расписаний в Google Sheets: {e}")
        
        return False
    
    def load_schedule_for_date(self, date: datetime, employee_manager=None) -> Dict[str, List[str]]:
        """Загрузить расписание на конкретную дату"""
        date_str = date.strftime('%Y-%m-%d')
        schedule = {}
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки
        if USE_POSTGRESQL:
            try:
                from database_sync import load_schedule_from_db_sync
                logger.debug(f"Используем синхронную загрузку расписания на {date_str} из PostgreSQL")
                db_schedule = load_schedule_from_db_sync(date_str)
                logger.debug("load_schedule_from_db_sync завершен успешно")
                
                if db_schedule:
                    # db_schedule имеет формат {day_name: employees_str}
                    for day_name, employees_str in db_schedule.items():
                        employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                        # Форматируем имена, если нужно
                        if employee_manager:
                            formatted_employees = []
                            for emp in employees:
                                # Проверяем, отформатировано ли уже имя
                                if '(@' in emp and emp.endswith(')'):
                                    formatted_employees.append(emp)
                                else:
                                    formatted_employees.append(employee_manager.format_employee_name(emp))
                            schedule[day_name] = formatted_employees
                        else:
                            schedule[day_name] = employees
                    
                    if schedule:
                        logger.debug(f"Загружено расписание для {date_str} из PostgreSQL")
                        return schedule
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания на {date_str} из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и PostgreSQL недоступен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            # Не проверяем буферизованные операции - работаем только с PostgreSQL
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
                rows = filter_empty_rows(rows)
                start_idx, has_header = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                
                # Ищем запись для нужной даты
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[0] and row[0].strip() == date_str:
                        # Нашли запись для этой даты
                        day_name = row[1].strip() if len(row) > 1 and row[1] else None
                        employees_str = row[2].strip() if len(row) > 2 and row[2] else ""
                        
                        if day_name and employees_str:
                            employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                            # Форматируем имена, если нужно
                            if employee_manager:
                                formatted_employees = []
                                for emp in employees:
                                    # Проверяем, отформатировано ли уже имя
                                    if '(@' in emp and emp.endswith(')'):
                                        formatted_employees.append(emp)
                                    else:
                                        formatted_employees.append(employee_manager.format_employee_name(emp))
                                schedule[day_name] = formatted_employees
                            else:
                                schedule[day_name] = employees
                            
                            if schedule:
                                logger.info(f"Загружено расписание для {date_str} из Google Sheets")
                                return schedule
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания на {date_str} из Google Sheets: {e}, используем расписание по умолчанию")
        
        # Если файла нет и в Google Sheets нет, возвращаем расписание по умолчанию
        default_schedule = self.load_default_schedule()
        # Конвертируем из формата JSON (словарь мест) в формат списка для обратной совместимости
        default_schedule_list = self._default_schedule_to_list(default_schedule)
        # Форматируем имена в расписании по умолчанию, если есть employee_manager
        if employee_manager:
            formatted_default = {}
            for day, employees in default_schedule_list.items():
                formatted_default[day] = [employee_manager.format_employee_name(emp) for emp in employees]
            return formatted_default
        return default_schedule_list
    
    def save_schedule_for_week(self, week_start: datetime, schedule: Dict[str, List[str]], 
                              only_changed_days: bool = False, employee_manager=None,
                              changed_days=None):
        """
        Сохранить расписание на неделю в PostgreSQL, Google Sheets и файлы
        
        Args:
            week_start: Начало недели
            schedule: Расписание в формате {day_name: [имена]}
            only_changed_days: Если True, сохранять только дни, отличающиеся от default_schedule
            employee_manager: Менеджер сотрудников для форматирования имен
            changed_days: Множество имен дней, которые были изменены через requests (например, {'Понедельник', 'Вторник'})
        """
        from datetime import datetime as dt
        import pytz
        from config import TIMEZONE
        
        week_dates = self.get_week_dates(week_start)
        timezone = pytz.timezone(TIMEZONE)
        now = dt.now(timezone)
        today = now.date()
        
        # Загружаем default_schedule для сравнения
        default_schedule = self.load_default_schedule()
        default_schedule_list = self._default_schedule_to_list(default_schedule)
        
        # Форматируем имена в default_schedule для сравнения
        if employee_manager:
            formatted_default = {}
            for day, employees in default_schedule_list.items():
                formatted_default[day] = [employee_manager.format_employee_name(emp) for emp in employees]
        else:
            formatted_default = default_schedule_list
        
        # Сохраняем в PostgreSQL (приоритет 1)
        pool = _get_pool()
        if USE_POSTGRESQL and pool and save_schedule_to_db:
            # Собираем все даты недели для проверки существующих записей
            week_date_strs = []
            for date, day_name in week_dates:
                date_obj = date.date()
                if date_obj > today:  # Только будущие даты
                    week_date_strs.append(date.strftime('%Y-%m-%d'))
            
            # Загружаем существующие расписания для этой недели
            existing_schedules = set()
            if only_changed_days and changed_days is not None:
                try:
                    from database_sync import load_schedule_from_db_sync
                    for date_str in week_date_strs:
                        existing = load_schedule_from_db_sync(date_str)
                        if existing:
                            existing_schedules.add(date_str)
                    logger.info(f"Найдено существующих расписаний для недели {week_start.strftime('%Y-%m-%d')}: {existing_schedules}")
                except Exception as e:
                    logger.warning(f"Ошибка загрузки существующих расписаний: {e}")
            
            for date, day_name in week_dates:
                date_obj = date.date()
                
                # Пропускаем текущую и прошлые недели
                if date_obj <= today:
                    continue
                
                date_str = date.strftime('%Y-%m-%d')
                employees = schedule.get(day_name, [])
                default_employees = formatted_default.get(day_name, [])
                
                # Сортируем для сравнения
                employees_sorted = sorted([e.strip() for e in employees if e.strip()])
                default_employees_sorted = sorted([e.strip() for e in default_employees if e.strip()])
                
                # Проверяем, отличается ли расписание от default
                is_different = employees_sorted != default_employees_sorted
                
                if only_changed_days:
                    # Сохраняем только если:
                    # 1. День был явно изменен через requests (если changed_days указан) - сохраняем ВСЕГДА, даже если совпадает с default
                    # 2. ИЛИ день отличается от default (если changed_days не указан)
                    should_save = False
                    should_delete = False
                    if changed_days is not None:
                        # Сохраняем все дни, которые были изменены через requests (даже если результат совпадает с default)
                        day_in_changed = day_name in changed_days
                        should_save = day_in_changed  # Убираем проверку is_different - сохраняем все измененные дни
                        # Удаляем дни, которых нет в changed_days, но они есть в schedules
                        should_delete = not day_in_changed and date_str in existing_schedules
                        logger.info(f"Проверка дня {date_str} ({day_name}): в changed_days={day_in_changed}, отличается от default={is_different}, should_save={should_save}, should_delete={should_delete}")
                        if day_in_changed:
                            logger.info(f"  changed_days содержит: {changed_days}")
                            logger.info(f"  schedule: {employees_sorted}")
                            logger.info(f"  default: {default_employees_sorted}")
                    else:
                        # Старое поведение: сохраняем все отличающиеся дни
                        should_save = is_different
                        logger.info(f"Проверка дня {date_str} ({day_name}): отличается от default={is_different}, should_save={should_save}")
                    
                    if should_save:
                        employees_str = ', '.join(employees)
                        try:
                            from database_sync import save_schedule_to_db_sync
                            logger.info(f"🔄 Сохранение расписания для {date_str} ({day_name}) в PostgreSQL...")
                            result = save_schedule_to_db_sync(date_str, day_name, employees_str)
                            if result:
                                logger.info(f"✅ Сохранено измененное расписание для {date_str} ({day_name}) в PostgreSQL")
                            else:
                                logger.warning(f"⚠️ Не удалось сохранить расписание для {date_str} ({day_name}) в PostgreSQL (вернуло False)")
                        except Exception as e:
                            logger.error(f"❌ Ошибка сохранения расписания {date_str} в PostgreSQL: {e}", exc_info=True)
                    elif should_delete:
                        # Удаляем день, который не был изменен через requests, но есть в schedules
                        try:
                            from database_sync import delete_schedule_from_db_sync
                            logger.info(f"🗑️ Удаление расписания для {date_str} ({day_name}) из PostgreSQL (не в requests)...")
                            result = delete_schedule_from_db_sync(date_str)
                            if result:
                                logger.info(f"✅ Удалено расписание для {date_str} ({day_name}) из PostgreSQL")
                            else:
                                logger.warning(f"⚠️ Не удалось удалить расписание для {date_str} ({day_name}) из PostgreSQL")
                        except Exception as e:
                            logger.error(f"❌ Ошибка удаления расписания {date_str} из PostgreSQL: {e}", exc_info=True)
                    else:
                        logger.debug(f"Пропуск дня {date_str} ({day_name}): не соответствует условиям сохранения и не требует удаления")
                else:
                    # Сохраняем все дни (включая совпадающие с default) - используется при рассылке
                    employees_str = ', '.join(employees)
                    try:
                        from database_sync import save_schedule_to_db_sync
                        logger.debug(f"💾 Сохранение расписания для {date_str} ({day_name}) в PostgreSQL (only_changed_days=False, все дни)")
                        save_schedule_to_db_sync(date_str, day_name, employees_str)
                        logger.debug(f"✅ Сохранено расписание для {date_str} ({day_name}) в PostgreSQL")
                    except Exception as e:
                        logger.error(f"Ошибка сохранения расписания {date_str} в PostgreSQL: {e}", exc_info=True)
        #     try:
        #         rows_to_save = []
        #         for date, day_name in week_dates:
        #             date_str = date.strftime('%Y-%m-%d')
        #             employees = schedule.get(day_name, [])
        #             employees_str = ', '.join(employees)
        #             rows_to_save.append([date_str, day_name, employees_str])
        #         
        #         # Обновляем записи для этой недели
        #         worksheet = self.sheets_manager.get_worksheet(SHEET_SCHEDULES)
        #         if worksheet:
        #             all_rows = worksheet.get_all_values()
        #             all_rows = filter_empty_rows(all_rows)
        #             
        #             # Получаем даты недели
        #             week_dates_str = [d.strftime('%Y-%m-%d') for d, _ in week_dates]
        #             
        #             # Пропускаем заголовок, если есть
        #             start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
        #             rows_to_keep = [all_rows[0]] if has_header else [['date', 'day_name', 'employees']]
        #             
        #             # Оставляем только записи не для этой недели
        #             for row in all_rows[start_idx:]:
        #                 if len(row) >= 1 and row[0] and row[0].strip() not in week_dates_str:
        #                     rows_to_keep.append(row)
        #             # Добавляем новые записи для этой недели
        #             rows_to_keep.extend(rows_to_save)
        #             # Перезаписываем весь лист
        #             self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_keep, clear_first=True)
        #     except Exception as e:
        #         logger.warning(f"Ошибка сохранения расписания недели в Google Sheets: {e}")
        
        # Сохраняем в файлы (только измененные дни, если only_changed_days=True)
        # Собираем существующие файлы для этой недели
        existing_files = set()
        if only_changed_days and changed_days is not None:
            for date, day_name in week_dates:
                date_obj = date.date()
                if date_obj > today:
                    date_str = date.strftime('%Y-%m-%d')
                    schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                    if os.path.exists(schedule_file):
                        existing_files.add(date_str)
        
        for date, day_name in week_dates:
            date_obj = date.date()
            
            # Пропускаем текущую и прошлые недели
            if date_obj <= today:
                continue
            
            date_str = date.strftime('%Y-%m-%d')
            employees = schedule.get(day_name, [])
            default_employees = formatted_default.get(day_name, [])
            
            # Сортируем для сравнения
            employees_sorted = sorted([e.strip() for e in employees if e.strip()])
            default_employees_sorted = sorted([e.strip() for e in default_employees if e.strip()])
            
            # Проверяем, отличается ли расписание от default
            is_different = employees_sorted != default_employees_sorted
            
            if only_changed_days:
                # Сохраняем только если:
                # 1. День был явно изменен через requests (если changed_days указан) - сохраняем ВСЕГДА, даже если совпадает с default
                # 2. ИЛИ день отличается от default (если changed_days не указан)
                should_save = False
                should_delete = False
                if changed_days is not None:
                    # Сохраняем все дни, которые были изменены через requests (даже если результат совпадает с default)
                    day_in_changed = day_name in changed_days
                    should_save = day_in_changed  # Убираем проверку is_different
                    # Удаляем дни, которых нет в changed_days, но они есть в файлах
                    should_delete = not day_in_changed and date_str in existing_files
                else:
                    # Старое поведение: сохраняем все отличающиеся дни
                    should_save = is_different
                
                if should_save:
                    schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                    try:
                        with open(schedule_file, 'w', encoding='utf-8') as f:
                            f.write(f"{date_str}\n")
                            f.write(f"{day_name}\n")
                            f.write(f"{', '.join(employees)}\n")
                    except Exception as e:
                        logger.error(f"Ошибка сохранения расписания {date_str} в файл: {e}")
                elif should_delete:
                    # Удаляем файл, который не был изменен через requests
                    schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                    try:
                        os.remove(schedule_file)
                        logger.info(f"🗑️ Удален файл расписания для {date_str} ({day_name}) - не в requests")
                    except Exception as e:
                        logger.warning(f"Не удалось удалить файл расписания для {date_str}: {e}")
            else:
                # Сохраняем все дни (старое поведение)
                schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                try:
                    with open(schedule_file, 'w', encoding='utf-8') as f:
                        f.write(f"{date_str}\n")
                        f.write(f"{day_name}\n")
                        f.write(f"{', '.join(employees)}\n")
                except Exception as e:
                    logger.error(f"Ошибка сохранения расписания {date_str} в файл: {e}")
    
    def update_schedule_for_date(self, date: datetime, employee_name: str, 
                                 action: str, employee_manager):
        """
        Обновить расписание на конкретную дату (для текущей недели)
        action: 'remove' или 'add'
        Возвращает: (успех, количество свободных мест после операции)
        """
        date_str = date.strftime('%Y-%m-%d')
        schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
        
        # Определяем день недели
        week_dates = self.get_week_dates(self.get_week_start(date))
        day_name = None
        for d, day_n in week_dates:
            if d.date() == date.date():
                day_name = day_n
                break
        
        if not day_name:
            return False, 0
        
        # Загружаем текущее расписание для этой даты
        schedule = self.load_schedule_for_date(date, employee_manager)
        
        if day_name not in schedule:
            schedule[day_name] = []
        
        employees = schedule[day_name].copy()
        formatted_name = employee_manager.format_employee_name(employee_name)
        
        if action == 'remove':
            # Удаляем сотрудника
            employees = [emp for emp in employees if emp != formatted_name]
            # После удаления проверяем очередь и добавляем первого, если есть место
            # (это будет вызвано из process_queue_for_date после сохранения)
        elif action == 'add':
            # Проверяем, есть ли уже сотрудник
            if formatted_name not in employees:
                # Проверяем, есть ли место
                if len(employees) < MAX_OFFICE_SEATS:
                    employees.append(formatted_name)
                else:
                    return False, 0  # Нет места
        
        # Формируем строку для сохранения
        employees_str = ', '.join(employees)
        
        # Сохраняем в PostgreSQL ПЕРВЫМ (приоритет 1)
        pool = _get_pool()
        logger.info(f"🔄 Начинаю сохранение расписания {date_str} ({day_name}) в PostgreSQL...")
        logger.info(f"   USE_POSTGRESQL={USE_POSTGRESQL}, _pool={pool is not None}, save_schedule_to_db={save_schedule_to_db is not None}")
        if USE_POSTGRESQL and pool and save_schedule_to_db:
            try:
                logger.info(f"   Выполняю save_schedule_to_db({date_str}, {day_name}, {len(employees_str)} символов)...")
                # Используем синхронную функцию для записи
                from database_sync import save_schedule_to_db_sync
                logger.info(f"   Используем синхронное сохранение расписания в PostgreSQL...")
                result = save_schedule_to_db_sync(date_str, day_name, employees_str)
                logger.info(f"   Получен результат: {result}")
                if result:
                    logger.info(f"✅ Расписание {date_str} ({day_name}) сохранено в PostgreSQL")
                else:
                    logger.warning(f"⚠️ Расписание {date_str} ({day_name}) не сохранено в PostgreSQL (вернуло False)")
                    # Не обновляем память, если не удалось сохранить в PostgreSQL
                    return False, 0
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при сохранении расписания {date_str} в PostgreSQL: {e}", exc_info=True)
                # Не обновляем память, если произошла ошибка
                return False, 0
        else:
            pool = _get_pool()
            logger.warning(f"⚠️ PostgreSQL недоступен для сохранения расписания {date_str}: USE_POSTGRESQL={USE_POSTGRESQL}, _pool={pool is not None}, save_schedule_to_db={save_schedule_to_db is not None}")
            # Не обновляем память, если PostgreSQL недоступен
            return False, 0
        
        # Обновляем память только после успешного сохранения в PostgreSQL
        schedule[day_name] = employees
        #     try:
        #         logger.debug(f"Сохранение расписания в Google Sheets для {date_str}, день: {day_name}")
        #         # Сохраняем только измененный день (как в файле)
        #         row = [date_str, day_name, employees_str]
        #         
        #         # Обновляем записи в Google Sheets
        #         worksheet = self.sheets_manager.get_worksheet(SHEET_SCHEDULES)
        #         if worksheet:
        #             all_rows = worksheet.get_all_values()
        #             all_rows = filter_empty_rows(all_rows)
        #             
        #             # Пропускаем заголовок, если есть
        #             start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
        #             rows_to_keep = [all_rows[0]] if has_header else [['date', 'day_name', 'employees']]
        #             
        #             # Оставляем только записи не для этой даты и дня
        #             found = False
        #             for row_data in all_rows[start_idx:]:
        #                 if len(row_data) >= 2 and row_data[0] and row_data[0].strip() == date_str and row_data[1] and row_data[1].strip() == day_name:
        #                     # Это запись для этой даты и дня - заменяем её
        #                     found = True
        #                     logger.info(f"Найдена существующая запись для {date_str} {day_name}, заменяю")
        #                     rows_to_keep.append(row)
        #                 elif len(row_data) >= 1 and row_data[0] != date_str:
        #                     # Запись для другой даты - оставляем
        #                     rows_to_keep.append(row_data)
        #             
        #             # Если не нашли существующую запись, добавляем новую
        #             if not found:
        #                 logger.info(f"Не найдена существующая запись для {date_str} {day_name}, добавляю новую")
        #                 rows_to_keep.append(row)
        #             
        #             # Перезаписываем весь лист
        #             logger.info(f"Сохраняю {len(rows_to_keep)} строк в Google Sheets (включая заголовок)")
        #             logger.info(f"Данные для сохранения: date={date_str}, day={day_name}, employees={employees_str[:100]}")
        #             self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_keep, clear_first=True)
        #             logger.info(f"✅ Расписание успешно сохранено в Google Sheets для {date_str}")
        #         else:
        #             logger.warning(f"Не удалось получить лист {SHEET_SCHEDULES}")
        #     except Exception as e:
        #         logger.error(f"Ошибка сохранения расписания в Google Sheets: {e}", exc_info=True)
        
        # Сохраняем в файл
        with open(schedule_file, 'w', encoding='utf-8') as f:
            f.write(f"{date_str}\n")
            f.write(f"{day_name}\n")
            f.write(f"{', '.join(employees)}\n")
        
        # Возвращаем количество свободных мест
        free_slots = MAX_OFFICE_SEATS - len(employees)
        return True, free_slots
    
    def add_to_queue(self, date: datetime, employee_name: str, telegram_id: int):
        """Добавить сотрудника в очередь на дату (PostgreSQL, Google Sheets, файл)"""
        date_str = date.strftime('%Y-%m-%d')
        
        # Проверяем, не в очереди ли уже
        queue = self.get_queue_for_date(date)
        for entry in queue:
            if entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id:
                logger.debug(f"  {employee_name} уже в очереди на {date_str}")
                return False  # Уже в очереди
        
        # Сохраняем в PostgreSQL (приоритет 1)
        # Используем синхронную функцию напрямую, так как она не требует пула
        if USE_POSTGRESQL:
            try:
                from database_sync import add_to_queue_db_sync
                logger.info(f"🔄 Добавление в очередь PostgreSQL: {employee_name} на {date_str}...")
                result = add_to_queue_db_sync(date_str, employee_name, telegram_id)
                if result:
                    logger.info(f"✅ Добавлено в очередь PostgreSQL: {employee_name} на {date_str}")
                else:
                    logger.warning(f"⚠️ Не удалось добавить в очередь PostgreSQL: {employee_name} на {date_str}")
            except Exception as e:
                logger.error(f"❌ Ошибка добавления в очередь в PostgreSQL: {e}", exc_info=True)
        #     try:
        #         row = [date_str, employee_name, str(telegram_id)]
        #         self.sheets_manager.append_row(SHEET_QUEUE, row)
        #     except Exception as e:
        #         logger.warning(f"Ошибка сохранения в очередь в Google Sheets: {e}")
        
        # Добавляем в очередь (файл)
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        try:
            with open(queue_file, 'a', encoding='utf-8') as f:
                f.write(f"{employee_name}:{telegram_id}\n")
        except Exception as e:
            logger.error(f"Ошибка сохранения в очередь в файл: {e}")
        return True
    
    def get_queue_for_date(self, date: datetime) -> List[Dict]:
        """Получить очередь на дату из PostgreSQL (приоритет), Google Sheets или файла"""
        date_str = date.strftime('%Y-%m-%d')
        queue = []
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки
        if USE_POSTGRESQL:
            try:
                from database_sync import load_queue_from_db_sync
                logger.debug(f"Используем синхронную загрузку очереди на {date_str} из PostgreSQL")
                db_queue = load_queue_from_db_sync(date_str)
                logger.debug("load_queue_from_db_sync завершен успешно")
                
                if db_queue:
                    queue = db_queue
                    logger.debug(f"Очередь для {date_str} загружена из PostgreSQL: {len(queue)} записей")
                    return queue
            except Exception as e:
                logger.warning(f"Ошибка загрузки очереди из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Локальные файлы
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        if os.path.exists(queue_file):
            try:
                with open(queue_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(':')
                        if len(parts) >= 2:
                            employee_name = parts[0]
                            telegram_id = int(parts[1])
                            queue.append({
                                'employee_name': employee_name,
                                'telegram_id': telegram_id
                            })
            except Exception as e:
                logger.error(f"Ошибка загрузки очереди: {e}")
        
        # ПРИОРИТЕТ 3: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и локальных файлов нет)
        if USE_GOOGLE_SHEETS_FOR_READS and not queue and self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_QUEUE)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[0] and row[0].strip() == date_str:
                        try:
                            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
                            telegram_id = int(row[2].strip()) if len(row) > 2 and row[2] else None
                            if employee_name and telegram_id:
                                queue.append({
                                    'employee_name': employee_name,
                                    'telegram_id': telegram_id
                                })
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                logger.warning(f"Ошибка загрузки очереди из Google Sheets: {e}")
        
        return queue
    
    def remove_from_queue(self, date: datetime, employee_name: str, telegram_id: int):
        """Удалить сотрудника из очереди на дату (PostgreSQL, Google Sheets, файл)"""
        date_str = date.strftime('%Y-%m-%d')
        
        logger.info(f"Удаление из очереди: {date_str}, сотрудник: {employee_name}, ID: {telegram_id}")
        
        queue = self.get_queue_for_date(date)
        logger.info(f"Очередь до удаления: {len(queue)} записей")
        
        # Удаляем сотрудника из очереди
        queue = [entry for entry in queue 
                if not (entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id)]
        
        logger.info(f"Очередь после удаления: {len(queue)} записей")
        
        # Удаляем из PostgreSQL (приоритет 1)
        # Используем синхронную функцию напрямую, так как она не требует пула
        if USE_POSTGRESQL:
            try:
                # Используем синхронную функцию для удаления
                from database_sync import remove_from_queue_db_sync
                result = remove_from_queue_db_sync(date_str, telegram_id)
                if result:
                    logger.info(f"✅ Удалено из очереди PostgreSQL: {employee_name} на {date_str}")
                else:
                    logger.warning(f"⚠️ Не удалось удалить из очереди PostgreSQL: {employee_name} на {date_str}")
            except Exception as e:
                logger.error(f"❌ Ошибка удаления из очереди в PostgreSQL: {e}", exc_info=True)
        else:
            logger.warning(f"⚠️ PostgreSQL недоступен для удаления из очереди: USE_POSTGRESQL={USE_POSTGRESQL}")
        #     try:
        #         # Удаляем все записи для этой даты и добавляем обновленные
        #         worksheet = self.sheets_manager.get_worksheet(SHEET_QUEUE)
        #         if worksheet:
        #             all_rows = worksheet.get_all_values()
        #             logger.info(f"Всего строк в Google Sheets: {len(all_rows)}")
        #             
        #             all_rows = filter_empty_rows(all_rows)
        #             
        #             # Пропускаем заголовок
        #             start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
        #             rows_to_keep = [all_rows[0]] if has_header else [['date', 'employee_name', 'telegram_id']]
        #             
        #             # Оставляем только записи не для этой даты
        #             for row in all_rows[start_idx:]:
        #                 if len(row) >= 1 and row[0] != date_str:
        #                     rows_to_keep.append(row)
        #             
        #             # Добавляем обновленные записи для этой даты (если очередь не пуста)
        #             for entry in queue:
        #                 rows_to_keep.append([date_str, entry['employee_name'], str(entry['telegram_id'])])
        #             
        #             logger.info(f"Сохраняю {len(rows_to_keep)} строк в Google Sheets (включая заголовок)")
        #             # Перезаписываем весь лист (даже если очередь пуста - это удалит запись)
        #             self.sheets_manager.write_rows(SHEET_QUEUE, rows_to_keep, clear_first=True)
        #             logger.info(f"Очередь обновлена в Google Sheets")
        #     except Exception as e:
        #         logger.error(f"Ошибка обновления очереди в Google Sheets: {e}", exc_info=True)
        
        # Сохраняем обновленную очередь в файл
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        if queue:
            with open(queue_file, 'w', encoding='utf-8') as f:
                for entry in queue:
                    f.write(f"{entry['employee_name']}:{entry['telegram_id']}\n")
        else:
            # Если очередь пуста, удаляем файл
            if os.path.exists(queue_file):
                os.remove(queue_file)
                logger.info(f"Файл очереди {queue_file} удален (очередь пуста)")
    
    def process_queue_for_date(self, date: datetime, employee_manager) -> Optional[Dict]:
        """
        Обработать очередь на дату - добавить первого из очереди, если есть место
        Возвращает информацию о добавленном сотруднике или None
        """
        queue = self.get_queue_for_date(date)
        if not queue:
            return None
        
        # Проверяем, есть ли место
        schedule = self.load_schedule_for_date(date, employee_manager)
        week_dates = self.get_week_dates(self.get_week_start(date))
        day_name = None
        for d, day_n in week_dates:
            if d.date() == date.date():
                day_name = day_n
                break
        
        if not day_name or day_name not in schedule:
            return None
        
        employees = schedule.get(day_name, [])
        if len(employees) >= MAX_OFFICE_SEATS:
            return None  # Нет места
        
        # Берем первого из очереди
        first_in_queue = queue[0]
        employee_name = first_in_queue['employee_name']
        
        # Добавляем в расписание
        success, _ = self.update_schedule_for_date(date, employee_name, 'add', employee_manager)
        
        if success:
            # Удаляем из очереди
            self.remove_from_queue(date, employee_name, first_in_queue['telegram_id'])
            return first_in_queue
        
        return None
    
    def save_request(self, employee_name: str, telegram_id: int, week_start: datetime,
                    days_requested: List[str], days_skipped: List[str]):
        """Сохранить заявку сотрудника в PostgreSQL, Google Sheets и файл"""
        week_str = week_start.strftime('%Y-%m-%d')
        
        # Удаляем дубликаты
        days_requested = list(dict.fromkeys(days_requested))  # Сохраняет порядок
        days_skipped = list(dict.fromkeys(days_skipped))
        
        days_req_str = ','.join(days_requested) if days_requested else ''
        days_skip_str = ','.join(days_skipped) if days_skipped else ''
        
        # Сохраняем в PostgreSQL (приоритет 1)
        # Сохраняем в PostgreSQL (приоритет 1)
        if USE_POSTGRESQL:
            try:
                from database_sync import save_request_to_db_sync
                logger.info(f"🔄 Начинаю сохранение заявки в PostgreSQL: {employee_name} (неделя {week_str})...")
                result = save_request_to_db_sync(week_str, employee_name, telegram_id, days_requested, days_skipped)
                if result:
                    logger.info(f"✅ Заявка сохранена в PostgreSQL: {employee_name} (неделя {week_str})")
                else:
                    logger.warning(f"⚠️ Заявка не сохранена в PostgreSQL (вернуло False): {employee_name} (неделя {week_str})")
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения заявки в PostgreSQL: {e}", exc_info=True)
        #     try:
        #         # Проверяем, есть ли заголовок, если нет - добавляем
        #         worksheet = self.sheets_manager.get_worksheet(SHEET_REQUESTS)
        #         if worksheet:
        #             all_rows = worksheet.get_all_values()
        #             all_rows = filter_empty_rows(all_rows)
        #             
        #             # Проверяем, есть ли заголовок
        #             _, has_header = get_header_start_idx(all_rows, ['week_start', 'week', 'Неделя', 'employee_name'])
        #             
        #             # Если заголовка нет, добавляем его
        #             if not has_header:
        #                 header = ['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped']
        #                 self.sheets_manager.write_rows(SHEET_REQUESTS, [header], clear_first=True)
        #                 logger.debug(f"Добавлен заголовок в лист {SHEET_REQUESTS}")
        #         
        #         # Формируем строку для таблицы: [week_start, employee_name, telegram_id, days_requested, days_skipped]
        #         row = [week_str, employee_name, str(telegram_id), days_req_str, days_skip_str]
        #         self.sheets_manager.append_row(SHEET_REQUESTS, row)
        #     except Exception as e:
        #         logger.warning(f"Ошибка сохранения заявки в Google Sheets: {e}")
        
        # Не сохраняем в файл - только PostgreSQL
    
    def load_requests_for_week(self, week_start: datetime) -> List[Dict]:
        """Загрузить все заявки на неделю из PostgreSQL (приоритет), Google Sheets или файла (схлопывает дубликаты)"""
        week_str = week_start.strftime('%Y-%m-%d')
        requests_dict = {}  # Ключ: (employee_name, telegram_id), значение: заявка
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки
        if USE_POSTGRESQL:
            try:
                from database_sync import load_requests_from_db_sync
                logger.debug(f"Используем синхронную загрузку заявок на неделю {week_str} из PostgreSQL")
                db_requests = load_requests_from_db_sync(week_str)
                logger.debug("load_requests_from_db_sync завершен успешно")
                
                if db_requests:
                    for req in db_requests:
                        key = (req['employee_name'], req['telegram_id'])
                        # Если уже есть заявка для этого сотрудника, объединяем
                        if key in requests_dict:
                            existing = requests_dict[key]
                            combined_requested = list(dict.fromkeys(existing['days_requested'] + req['days_requested']))
                            combined_skipped = list(dict.fromkeys(existing['days_skipped'] + req['days_skipped']))
                            combined_requested = [d for d in combined_requested if d not in combined_skipped]
                            requests_dict[key] = {
                                'employee_name': req['employee_name'],
                                'telegram_id': req['telegram_id'],
                                'days_requested': combined_requested,
                                'days_skipped': combined_skipped,
                                'created_at': _merge_request_created_at(
                                    existing.get('created_at'), req.get('created_at')
                                ),
                            }
                        else:
                            requests_dict[key] = req
                    
                    if requests_dict:
                        result = sorted(
                            requests_dict.values(),
                            key=_request_sort_key_for_week,
                        )
                        logger.debug(f"Заявки для недели {week_str} загружены из PostgreSQL: {len(result)} записей")
                        return result
            except Exception as e:
                logger.warning(f"Ошибка загрузки заявок из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и PostgreSQL недоступен)
        if USE_GOOGLE_SHEETS_FOR_READS and not requests_dict and self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_REQUESTS)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['week_start', 'week', 'Неделя', 'employee_name'])
                
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[0] and row[0].strip() == week_str:
                        try:
                            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
                            telegram_id = int(row[2].strip()) if len(row) > 2 and row[2] else None
                            days_requested_str = row[3].strip() if len(row) > 3 and row[3] else None
                            days_skipped_str = row[4].strip() if len(row) > 4 and row[4] else None
                            
                            if employee_name and telegram_id:
                                days_requested = [d.strip() for d in days_requested_str.split(',')] if days_requested_str else []
                                days_skipped = [d.strip() for d in days_skipped_str.split(',')] if days_skipped_str else []
                                
                                key = (employee_name, telegram_id)
                                if key in requests_dict:
                                    existing = requests_dict[key]
                                    combined_requested = list(dict.fromkeys(existing['days_requested'] + days_requested))
                                    combined_skipped = list(dict.fromkeys(existing['days_skipped'] + days_skipped))
                                    combined_requested = [d for d in combined_requested if d not in combined_skipped]
                                    requests_dict[key] = {
                                        'employee_name': employee_name,
                                        'telegram_id': telegram_id,
                                        'days_requested': combined_requested,
                                        'days_skipped': combined_skipped,
                                        'created_at': existing.get('created_at'),
                                    }
                                else:
                                    requests_dict[key] = {
                                        'employee_name': employee_name,
                                        'telegram_id': telegram_id,
                                        'days_requested': days_requested,
                                        'days_skipped': days_skipped,
                                        'created_at': None,
                                    }
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                logger.warning(f"Ошибка загрузки заявок из Google Sheets: {e}")
        
        return sorted(requests_dict.values(), key=_request_sort_key_for_week)
    
    def clear_requests_for_week(self, week_start: datetime):
        """Очистить заявки на неделю (после формирования расписания) в PostgreSQL, Google Sheets и файл"""
        week_str = week_start.strftime('%Y-%m-%d')
        
        # Удаляем из PostgreSQL (приоритет 1)
        if USE_POSTGRESQL:
            try:
                from database_sync import clear_requests_from_db_sync
                clear_requests_from_db_sync(week_str)
            except Exception as e:
                logger.warning(f"Ошибка очистки заявок в PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        #     try:
        #         worksheet = self.sheets_manager.get_worksheet(SHEET_REQUESTS)
        #         if worksheet:
        #             all_rows = worksheet.get_all_values()
        #             all_rows = filter_empty_rows(all_rows)
        #             
        #             # Пропускаем заголовок
        #             start_idx, has_header = get_header_start_idx(all_rows, ['week_start', 'week', 'Неделя', 'employee_name'])
        #             rows_to_keep = [all_rows[0]] if has_header else [['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped']]
        #             
        #             # Оставляем только записи не для этой недели
        #             for row in all_rows[start_idx:]:
        #                 if len(row) >= 1 and row[0] and row[0].strip() != week_str:
        #                     rows_to_keep.append(row)
        #             # Перезаписываем весь лист
        #             self.sheets_manager.write_rows(SHEET_REQUESTS, rows_to_keep, clear_first=True)
        #     except Exception as e:
        #         logger.warning(f"Ошибка очистки заявок в Google Sheets: {e}")
        
        # Не удаляем файлы - работаем только с PostgreSQL
    
    def _calculate_employee_days_count(self, default_schedule: Dict[str, Dict[str, str]], employee_name: str) -> int:
        """
        Подсчитать количество дней в неделю для сотрудника в расписании по умолчанию
        
        Args:
            default_schedule: Расписание по умолчанию в формате {день: {место: имя}}
            employee_name: Имя сотрудника
            
        Returns:
            int: Количество дней в неделю
        """
        count = 0
        for day_name, places_dict in default_schedule.items():
            for place_key, name in places_dict.items():
                plain_name = self.get_plain_name_from_formatted(name)
                if plain_name == employee_name:
                    count += 1
                    break
        return count
    
    def _assign_fixed_places(self, default_schedule: Dict[str, Dict[str, str]], 
                             schedule: Dict[str, Dict[str, str]], 
                             employee_manager) -> Dict[str, str]:
        """
        Назначить фиксированные места сотрудникам на основе приоритета (количество дней в неделю)
        
        Args:
            default_schedule: Расписание по умолчанию
            schedule: Текущее расписание (будет изменено)
            employee_manager: Менеджер сотрудников
            
        Returns:
            Dict[str, str]: Маппинг {имя_сотрудника: место} (например, {"Вася": "1.1"})
        """
        # Собираем всех сотрудников из default_schedule
        employees_info = {}  # {имя: {дни: {день: место}, days_count: количество}}
        
        for day_name, places_dict in default_schedule.items():
            for place_key, name in places_dict.items():
                plain_name = self.get_plain_name_from_formatted(name)
                if plain_name:
                    if plain_name not in employees_info:
                        employees_info[plain_name] = {
                            'days': {},
                            'days_count': 0
                        }
                    employees_info[plain_name]['days'][day_name] = place_key
        
        # Подсчитываем количество дней для каждого сотрудника
        for employee_name in employees_info:
            employees_info[employee_name]['days_count'] = len(employees_info[employee_name]['days'])
        
        # Сортируем сотрудников по количеству дней (по убыванию), затем по месту из первого дня, затем по имени
        # Имя сотрудника добавляется для стабильности сортировки - чтобы при одинаковом приоритете
        # сотрудники всегда получали места в одном и том же порядке
        sorted_employees = sorted(
            employees_info.items(),
            key=lambda x: (
                -x[1]['days_count'],  # Сначала по количеству дней (по убыванию)
                int(list(x[1]['days'].values())[0].split('.')[0]) if x[1]['days'] else 999,  # Затем по подразделению
                int(list(x[1]['days'].values())[0].split('.')[1]) if x[1]['days'] else 999,  # Затем по месту
                x[0]  # Затем по имени сотрудника (для стабильности)
            )
        )
        
        # Назначаем фиксированные места
        employee_to_place = {}  # {имя: место}
        place_to_employee = {}  # {место: имя} - для отслеживания конфликтов
        
        for employee_name, info in sorted_employees:
            days_dict = info['days']  # {день: место}
            days_list = list(days_dict.keys())
            
            # Находим место, которое сотрудник занимает в большинстве дней (или первое место)
            place_counts = {}
            for day, place in days_dict.items():
                place_counts[place] = place_counts.get(place, 0) + 1
            
            # Выбираем место, которое встречается чаще всего (или первое, если равны)
            most_common_place = max(place_counts.items(), key=lambda x: (x[1], -int(x[0].split('.')[0]), -int(x[0].split('.')[1])))[0]
            
            # Пытаемся использовать это место
            assigned_place = None
            
            # Проверяем, не занято ли это место сотрудником с более высоким приоритетом
            if most_common_place not in place_to_employee:
                # Место свободно - используем его
                assigned_place = most_common_place
            else:
                # Место занято - ищем свободное место
                # Ищем первое свободное место (начинаем с первого подразделения)
                for i in range(1, MAX_OFFICE_SEATS + 1):
                    candidate_place = f'1.{i}'
                    if candidate_place not in place_to_employee:
                        assigned_place = candidate_place
                        break
            
            if assigned_place:
                employee_to_place[employee_name] = assigned_place
                place_to_employee[assigned_place] = employee_name
                # Назначаем место сотруднику во все его дни
                # Сначала удаляем сотрудника из всех мест, где он мог быть (из default_schedule)
                for day in days_list:
                    if day in schedule:
                        # Удаляем сотрудника из всех мест этого дня, где он мог быть
                        removed_from_places = []
                        for place_key in list(schedule[day].keys()):
                            existing_name = schedule[day].get(place_key, '')
                            if existing_name:
                                plain_existing = self.get_plain_name_from_formatted(existing_name)
                                if plain_existing == employee_name:
                                    schedule[day][place_key] = ''
                                    removed_from_places.append(place_key)
                        if removed_from_places:
                            logger.debug(f"_assign_fixed_places: удален {employee_name} из мест {removed_from_places} в день {day}")
                        # Назначаем сотрудника на фиксированное место
                        # Проверяем, не занято ли уже это место другим сотрудником
                        existing_at_place = schedule[day].get(assigned_place, '')
                        if existing_at_place:
                            plain_existing = self.get_plain_name_from_formatted(existing_at_place)
                            if plain_existing != employee_name:
                                logger.warning(f"_assign_fixed_places: место {assigned_place} в день {day} уже занято {plain_existing}, но назначаем {employee_name}")
                        schedule[day][assigned_place] = employee_name
        
        return employee_to_place
    
    def build_schedule_from_requests(self, week_start: datetime, 
                                     requests: List[Dict],
                                     employee_manager) -> tuple[Dict[str, List[str]], Dict[str, set]]:
        """
        Построить расписание на основе заявок по новому алгоритму:
        1. Начинаем с default_schedule
        2. Применяем days_skipped - удаляем сотрудников из дней, которые они пропустили
        3. Применяем days_requested - добавляем сотрудников в запрошенные дни, но только если занято <= 7 мест
           Если занято 8 мест, запрос должен идти в queue (обрабатывается отдельно)
        
        Returns:
            tuple[Dict[str, List[str]], Dict[str, set]] - расписание и словарь удаленных через days_skipped
        """
        # Шаг 1: Начинаем с расписания по умолчанию
        default_schedule = self.load_default_schedule()
        
        # Копируем default_schedule в schedule (в формате словаря мест)
        schedule = {}
        for day_name, places_dict in default_schedule.items():
            schedule[day_name] = places_dict.copy()
        
        # Отслеживаем, какие сотрудники были удалены через days_skipped для каждого дня
        removed_by_skipped = {}  # {day: set(employee_names)}
        for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
            removed_by_skipped[day_name] = set()
        
        # Шаг 2: Применяем days_skipped - удаляем сотрудников из дней, которые они пропустили
        for req in requests:
            employee_name = req['employee_name']
            days_skipped = req['days_skipped']
            
            if not days_skipped:
                continue
            
            logger.info(f"build_schedule_from_requests: {employee_name}, пропущенные дни: {days_skipped}")
            
            for day in days_skipped:
                if day not in schedule:
                    continue
                
                # Ищем сотрудника в расписании на этот день
                place_key = self._find_employee_in_places(schedule[day], employee_name)
                if place_key:
                    # Удаляем сотрудника из расписания
                    schedule[day][place_key] = ''
                    removed_by_skipped[day].add(employee_name)
                    logger.info(f"  ✅ УДАЛЯЕМ {employee_name} из {day} (указан в days_skipped)")
        
        # Шаг 2.5: После удаления через days_skipped заполняем освободившиеся места из очереди
        # Получаем даты недели для работы с очередью
        week_dates = self.get_week_dates(week_start)
        day_to_date = {}
        for date, day_name in week_dates:
            day_to_date[day_name] = date
        
        # Для каждого дня проверяем очередь и заполняем освободившиеся места
        for day_name, date in day_to_date.items():
            if day_name not in schedule:
                continue
            
            # Проверяем, сколько мест занято после применения days_skipped
            occupied_count = len([name for name in schedule[day_name].values() if name])
            
            # Если есть свободные места (занято < 8), проверяем очередь
            while occupied_count < MAX_OFFICE_SEATS:
                # Получаем очередь на этот день
                queue = self.get_queue_for_date(date)
                if not queue:
                    # Очередь пуста - выходим
                    break
                
                # Берем первого из очереди
                first_in_queue = queue[0]
                queue_employee_name = first_in_queue['employee_name']
                
                # Проверяем, не добавлен ли уже этот сотрудник в расписание
                place_key = self._find_employee_in_places(schedule[day_name], queue_employee_name)
                if place_key:
                    # Сотрудник уже в расписании - удаляем из очереди и берем следующего
                    logger.info(f"  ⚠️ {queue_employee_name} уже в расписании на {day_name}, удаляем из очереди")
                    self.remove_from_queue(date, queue_employee_name, first_in_queue['telegram_id'])
                    continue
                
                # Добавляем первого из очереди в расписание
                free_place = self._find_free_place(schedule[day_name], department=1)
                if free_place:
                    schedule[day_name][free_place] = queue_employee_name
                    occupied_count += 1
                    # Удаляем из очереди
                    self.remove_from_queue(date, queue_employee_name, first_in_queue['telegram_id'])
                    logger.info(f"  ✅ Добавлен {queue_employee_name} из очереди в {day_name} на место {free_place}")
                else:
                    # Не должно быть такого случая
                    logger.warning(f"  ⚠️ Не найдено свободное место для {queue_employee_name} из очереди в {day_name}")
                    break
        
        # Шаг 3: Применяем days_requested - добавляем сотрудников в запрошенные дни
        # Но только если в этот день занято не больше 7 мест
        # Если занято 8 мест, запрос идет в queue
        for req in requests:
            employee_name = req['employee_name']
            telegram_id = req.get('telegram_id')
            days_requested = req['days_requested']
            
            if not days_requested:
                continue
            
            logger.info(f"build_schedule_from_requests: {employee_name}, запрошенные дни: {days_requested}, telegram_id: {telegram_id}")
            
            for day in days_requested:
                if day not in schedule:
                    continue
                
                # Получаем дату для этого дня
                date = day_to_date.get(day)
                if not date:
                    logger.warning(f"  ⚠️ Не найдена дата для дня {day}")
                    continue
                
                # Проверяем, есть ли уже сотрудник в расписании
                place_key = self._find_employee_in_places(schedule[day], employee_name)
                if place_key:
                    # Сотрудник уже в расписании - возможно, он был добавлен из очереди
                    # Проверяем, есть ли он в очереди, и если да - удаляем из очереди
                    queue = self.get_queue_for_date(date)
                    in_queue = any(entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id for entry in queue)
                    if in_queue:
                        logger.info(f"  {employee_name} уже в расписании на {day} (был добавлен из очереди), удаляем из очереди")
                        self.remove_from_queue(date, employee_name, telegram_id)
                    else:
                        logger.debug(f"  {employee_name} уже в расписании на {day}")
                    continue
                
                # Проверяем, был ли сотрудник в default_schedule на этот день
                # Если да, и его место занято кем-то из очереди, он должен попасть в очередь
                employee_was_in_default = False
                employee_default_place = None
                if day in default_schedule:
                    places_dict = default_schedule[day]
                    for place_key, name in places_dict.items():
                        plain_name_in_schedule = self.get_plain_name_from_formatted(name)
                        if plain_name_in_schedule == employee_name:
                            employee_was_in_default = True
                            employee_default_place = place_key
                            logger.info(f"  🔍 {employee_name} был в default_schedule на {day}, его место: {employee_default_place}")
                            break
                
                # Проверяем, занято ли его место из default_schedule кем-то другим
                if employee_was_in_default and employee_default_place:
                    # schedule[day] - это словарь мест {place_key: employee_name}
                    current_occupant = schedule[day].get(employee_default_place, '').strip()
                    logger.info(f"  🔍 Проверка места {employee_default_place}: текущий занят = '{current_occupant}', employee_name = '{employee_name}'")
                    if current_occupant:
                        # Место занято - проверяем, не самим ли сотрудником
                        # current_occupant - это простое имя (без форматирования)
                        logger.info(f"  🔍 Сравнение: '{current_occupant}' != '{employee_name}' = {current_occupant != employee_name}")
                        if current_occupant != employee_name:
                            # Место занято другим сотрудником (возможно, из очереди)
                            # Сотрудник должен попасть в очередь, а не занимать другое место
                            if telegram_id:
                                logger.info(f"  ⚠️ Место {employee_default_place} сотрудника {employee_name} из default_schedule занято {current_occupant}, добавляем в очередь")
                                result = self.add_to_queue(date, employee_name, telegram_id)
                                if result:
                                    logger.info(f"  ✅ {employee_name} успешно добавлен в очередь на {day}")
                                else:
                                    logger.warning(f"  ⚠️ Не удалось добавить {employee_name} в очередь на {day}")
                            else:
                                logger.warning(f"  ⚠️ Место {employee_default_place} занято, но нет telegram_id для добавления в очередь для {employee_name}")
                            continue
                        else:
                            logger.info(f"  ℹ️ Место {employee_default_place} занято самим сотрудником {employee_name} - продолжаем")
                    else:
                        logger.info(f"  ℹ️ Место {employee_default_place} свободно - можно вернуть сотрудника на его место")
                
                # Проверяем, сколько мест уже занято (после заполнения из очереди)
                occupied_count = len([name for name in schedule[day].values() if name])
                
                logger.info(f"  Проверка для {employee_name} в {day}: занято {occupied_count} из {MAX_OFFICE_SEATS} мест")
                
                if occupied_count >= MAX_OFFICE_SEATS:
                    # Все 8 мест заняты - добавляем в очередь
                    if telegram_id:
                        logger.info(f"  ⚠️ Все {MAX_OFFICE_SEATS} мест заняты в {day}, добавляем {employee_name} в очередь (telegram_id: {telegram_id})")
                        result = self.add_to_queue(date, employee_name, telegram_id)
                        if result:
                            logger.info(f"  ✅ {employee_name} успешно добавлен в очередь на {day}")
                        else:
                            logger.warning(f"  ⚠️ Не удалось добавить {employee_name} в очередь на {day}")
                    else:
                        logger.warning(f"  ⚠️ Все {MAX_OFFICE_SEATS} мест заняты в {day}, но нет telegram_id для добавления в очередь для {employee_name}")
                    continue
                
                # Если занято <= 7 мест, добавляем сотрудника
                # Если сотрудник был в default_schedule, пытаемся вернуть его на его место
                if employee_was_in_default and employee_default_place:
                    # Проверяем, свободно ли его место
                    if not schedule[day].get(employee_default_place, '').strip():
                        # Место свободно - возвращаем сотрудника на его место
                        schedule[day][employee_default_place] = employee_name
                        logger.info(f"  ✅ Возвращен {employee_name} в {day} на его место {employee_default_place} из default_schedule")
                    else:
                        # Место занято (не должно быть такого случая после проверки выше, но на всякий случай)
                        # Ищем свободное место
                        free_place = self._find_free_place(schedule[day], department=1)
                        if free_place:
                            schedule[day][free_place] = employee_name
                            logger.info(f"  ✅ Добавлен {employee_name} в {day} на место {free_place}")
                        else:
                            logger.warning(f"  ⚠️ Не найдено свободное место для {employee_name} в {day}, хотя занято {occupied_count} мест")
                else:
                    # Сотрудник не был в default_schedule - ищем свободное место
                    free_place = self._find_free_place(schedule[day], department=1)
                    if free_place:
                        schedule[day][free_place] = employee_name
                        logger.info(f"  ✅ Добавлен {employee_name} в {day} на место {free_place}")
                    else:
                        # Не должно быть такого случая, но на всякий случай
                        logger.warning(f"  ⚠️ Не найдено свободное место для {employee_name} в {day}, хотя занято {occupied_count} мест")
        
        # Конвертируем обратно в формат списка для вывода
        formatted_schedule = {}
        for day, places_dict in schedule.items():
            employees = self._get_employees_list_from_places(places_dict)
            # Форматируем имена с никнеймами для вывода
            formatted_schedule[day] = [employee_manager.format_employee_name(emp) for emp in employees]
        
        return formatted_schedule, removed_by_skipped
    
    def get_available_slots(self, schedule: Dict[str, List[str]]) -> Dict[str, int]:
        """Получить количество свободных мест по дням"""
        available = {}
        for day, employees in schedule.items():
            available[day] = MAX_OFFICE_SEATS - len(employees)
        return available
    
    def get_employee_schedule(self, week_start: datetime, employee_name: str, employee_manager=None) -> Dict[str, bool]:
        """Получить расписание сотрудника на неделю (True - в офисе, False - удаленно)"""
        schedule = self.load_schedule_for_date(week_start, employee_manager)
        week_dates = self.get_week_dates(week_start)
        
        employee_schedule = {}
        # Форматируем имя сотрудника для поиска
        formatted_name = employee_manager.format_employee_name(employee_name) if employee_manager else employee_name
        
        for date, day_name in week_dates:
            employees = schedule.get(day_name, [])
            # Проверяем, есть ли имя сотрудника в списке (может быть отформатированным)
            employee_schedule[day_name] = formatted_name in employees
        
        return employee_schedule
    
    def update_employee_name_in_default_schedule(self, old_name: str, new_formatted_name: str):
        """Обновить имя сотрудника в default_schedule (заменить простое имя на форматированное)"""
        # Загружаем текущее расписание (в новом формате JSON)
        schedule = self.load_default_schedule()
        
        # Обновляем имена в расписании
        updated = False
        for day_name, places_dict in schedule.items():
            for place_key, name in places_dict.items():
                # Извлекаем простое имя из отформатированного (если есть)
                plain_name = self.get_plain_name_from_formatted(name)
                # Если простое имя совпадает с old_name, заменяем на новое форматированное
                if plain_name == old_name:
                    schedule[day_name][place_key] = new_formatted_name
                    updated = True
        
        # Если были изменения, сохраняем обновленное расписание
        if updated:
            self.save_default_schedule(schedule)
    
    def update_employee_name_in_schedules(self, old_name: str, new_formatted_name: str):
        """Обновить имя сотрудника во всех расписаниях в PostgreSQL и Google Sheets"""
        from config import USE_POSTGRESQL
        from datetime import datetime, timedelta
        
        updated_count = 0
        
        # Обновляем в PostgreSQL
        if USE_POSTGRESQL:
            try:
                from database_sync import load_schedule_from_db_sync, save_schedule_to_db_sync
                
                # Проверяем последние 60 дней
                today = datetime.now().date()
                for i in range(60):
                    date = today + timedelta(days=i - 30)  # От -30 до +30 дней
                    date_str = date.strftime('%Y-%m-%d')
                    
                    db_schedule = load_schedule_from_db_sync(date_str)
                    if db_schedule:
                        for day_name, employees_str in db_schedule.items():
                            if employees_str:
                                employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                                updated_employees = []
                                row_updated = False
                                
                                for emp in employees:
                                    # Извлекаем простое имя из отформатированного (если есть)
                                    plain_name = self.get_plain_name_from_formatted(emp)
                                    if plain_name == old_name:
                                        # Заменяем на новое форматированное имя
                                        updated_employees.append(new_formatted_name)
                                        row_updated = True
                                    else:
                                        updated_employees.append(emp)
                                
                                if row_updated:
                                    new_employees_str = ', '.join(updated_employees)
                                    if save_schedule_to_db_sync(date_str, day_name, new_employees_str):
                                        updated_count += 1
                                        logger.debug(f"Обновлено имя '{old_name}' → '{new_formatted_name}' в расписании {date_str} ({day_name}) в PostgreSQL")
            except Exception as e:
                logger.error(f"Ошибка обновления имени сотрудника в расписаниях PostgreSQL: {e}", exc_info=True)
        
        # Обновляем в Google Sheets (только если включено)
        from config import USE_GOOGLE_SHEETS_FOR_WRITES
        if USE_GOOGLE_SHEETS_FOR_WRITES and self.sheets_manager and self.sheets_manager.is_available():
            try:
                from utils import filter_empty_rows, get_header_start_idx
                from config import SHEET_SCHEDULES
                
                rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
                rows = filter_empty_rows(rows)
                start_idx, has_header = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                
                updated = False
                rows_to_save = []
                
                # Сохраняем заголовок, если есть
                if has_header:
                    rows_to_save.append(rows[0])
                else:
                    rows_to_save.append(['date', 'day_name', 'employees'])
                
                # Обрабатываем все строки
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[2]:  # Проверяем, что есть список сотрудников
                        employees_str = row[2].strip()
                        employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                        
                        # Проверяем, есть ли старое имя в списке
                        updated_row = False
                        new_employees = []
                        for emp in employees:
                            # Извлекаем простое имя из отформатированного (если есть)
                            plain_name = self.get_plain_name_from_formatted(emp)
                            if plain_name == old_name:
                                # Заменяем на новое форматированное имя
                                new_employees.append(new_formatted_name)
                                updated_row = True
                                updated = True
                            else:
                                new_employees.append(emp)
                        
                        if updated_row:
                            # Обновляем строку с новым списком сотрудников
                            new_row = row.copy()
                            new_row[2] = ', '.join(new_employees)
                            rows_to_save.append(new_row)
                        else:
                            # Оставляем строку без изменений
                            rows_to_save.append(row)
                    else:
                        # Оставляем строку без изменений (некорректный формат)
                        rows_to_save.append(row)
                
                if updated:
                    self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_save, clear_first=True)
                    logger.info(f"Обновлено имя сотрудника '{old_name}' → '{new_formatted_name}' во всех расписаниях в Google Sheets")
            except Exception as e:
                logger.error(f"Ошибка обновления имени сотрудника в расписаниях Google Sheets: {e}", exc_info=True)
        
        if updated_count > 0:
            logger.info(f"✅ Обновлено {updated_count} расписаний в PostgreSQL для сотрудника '{old_name}' → '{new_formatted_name}'")
    
    def _update_all_employee_names_in_default_schedule(self):
        """Обновить все имена сотрудников в default_schedule.txt при старте бота"""
        if not self.employee_manager:
            return
        
        # Загружаем текущее расписание
        schedule = self.load_default_schedule()
        
        # Для каждого сотрудника обновляем имя в расписании
        for telegram_id in self.employee_manager.get_all_telegram_ids():
            employee_data = self.employee_manager.get_employee_data(telegram_id)
            if employee_data:
                manual_name, _, username = employee_data
                formatted_name = self.employee_manager.format_employee_name_by_id(telegram_id)
                # Обновляем имя в расписании
                self.update_employee_name_in_default_schedule(manual_name, formatted_name)
    
    def refresh_all_schedules_with_usernames(self):
        """
        Обновить все имена сотрудников в default_schedule и schedules на основе данных из employees.
        Используется для синхронизации после ручного добавления сотрудников в Google Sheets.
        
        Returns:
            tuple: (updated_default_count, updated_schedules_count) - количество обновленных записей
        """
        if not self.employee_manager:
            return 0, 0
        
        # Перезагружаем данные сотрудников из Google Sheets
        self.employee_manager.reload_employees()
        
        updated_default_count = 0
        updated_schedules_count = 0
        
        # Обновляем default_schedule
        default_schedule = self.load_default_schedule()
        for day_name, places_dict in default_schedule.items():
            for place_key, name in places_dict.items():
                if name:  # Если место не пустое
                    plain_name = self.get_plain_name_from_formatted(name)
                    # Ищем сотрудника по имени
                    telegram_id = self.employee_manager.get_employee_id(plain_name)
                    if telegram_id:
                        formatted_name = self.employee_manager.format_employee_name_by_id(telegram_id)
                        # Если имя изменилось (добавился username), обновляем
                        if formatted_name != name:
                            default_schedule[day_name][place_key] = formatted_name
                            updated_default_count += 1
        
        # Сохраняем обновленный default_schedule
        if updated_default_count > 0:
            self.save_default_schedule(default_schedule)
            logger.info(f"Обновлено {updated_default_count} имен в default_schedule")
        #     try:
        #         rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
        #         rows = filter_empty_rows(rows)
        #         start_idx, has_header = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
        #         
        #         rows_to_save = []
        #         
        #         # Сохраняем заголовок
        #         if has_header:
        #             rows_to_save.append(rows[0])
        #         else:
        #             rows_to_save.append(['date', 'day_name', 'employees'])
        #         
        #         # Обрабатываем все строки
        #         for row in rows[start_idx:]:
        #             if len(row) >= 3 and row[2]:  # Проверяем, что есть список сотрудников
        #                 employees_str = row[2].strip()
        #                 employees = [e.strip() for e in employees_str.split(',') if e.strip()]
        #                 
        #                 # Обновляем имена сотрудников
        #                 updated_row = False
        #                 new_employees = []
        #                 for emp in employees:
        #                     plain_name = self.get_plain_name_from_formatted(emp)
        #                     # Ищем сотрудника по имени
        #                     telegram_id = self.employee_manager.get_employee_id(plain_name)
        #                     if telegram_id:
        #                         formatted_name = self.employee_manager.format_employee_name_by_id(telegram_id)
        #                         # Если имя изменилось (добавился username), обновляем
        #                         if formatted_name != emp:
        #                             new_employees.append(formatted_name)
        #                             updated_row = True
        #                             updated_schedules_count += 1
        #                         else:
        #                             new_employees.append(emp)
        #                     else:
        #                         # Сотрудник не найден, оставляем как есть
        #                         new_employees.append(emp)
        #                 
        #                 if updated_row:
        #                     # Обновляем строку с новым списком сотрудников
        #                     new_row = row.copy()
        #                     new_row[2] = ', '.join(new_employees)
        #                     rows_to_save.append(new_row)
        #                 else:
        #                     # Оставляем строку без изменений
        #                     rows_to_save.append(row)
        #             else:
        #                 # Оставляем строку без изменений (некорректный формат)
        #                 rows_to_save.append(row)
        #         
        #         if updated_schedules_count > 0:
        #             self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_save, clear_first=True)
        #             logger.info(f"Обновлено {updated_schedules_count} имен в schedules")
        #     except Exception as e:
        #         logger.error(f"Ошибка обновления schedules: {e}")
        
        return updated_default_count, updated_schedules_count

