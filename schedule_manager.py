"""
Управление расписаниями
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
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
        self._save_default_schedule()
        # Обновляем имена в default_schedule.txt при старте, если есть employee_manager
        if employee_manager:
            self._update_all_employee_names_in_default_schedule()
    
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
        
        # Загружаем из файла
        if os.path.exists(DEFAULT_SCHEDULE_FILE):
            try:
                # Пытаемся загрузить как JSON
                with open(DEFAULT_SCHEDULE_FILE, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if content.startswith('{'):
                        # JSON формат
                        schedule = json.loads(content)
                    else:
                        # Старый формат (текстовый) - конвертируем
                        schedule = {}
                        current_day = None
                        for line in content.split('\n'):
                            line = line.strip()
                            if not line:
                                continue
                            if line in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                                current_day = line
                                schedule[current_day] = {}
                            elif current_day and ':' in line:
                                # Формат: "Понедельник: Вася, Дима Ч, ..."
                                if line.startswith(current_day + ':'):
                                    employees_str = line.split(':', 1)[1].strip()
                                    employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                                    places_dict = {}
                                    for i, emp in enumerate(employees, 1):
                                        places_dict[f'1.{i}'] = emp
                                    schedule[current_day] = places_dict
                                else:
                                    # Просто список через запятую
                                    employees = [e.strip() for e in line.split(',') if e.strip()]
                                    places_dict = {}
                                    for i, emp in enumerate(employees, 1):
                                        places_dict[f'1.{i}'] = emp
                                    schedule[current_day] = places_dict
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"Ошибка загрузки расписания по умолчанию: {e}")
        
        # Если не загрузилось из файла, пробуем загрузить из Google Sheets
        # (но только если USE_GOOGLE_SHEETS_FOR_READS включен и нет буферизованных операций)
        if USE_GOOGLE_SHEETS_FOR_READS and not schedule and self.sheets_manager and self.sheets_manager.is_available():
            # Проверяем, есть ли буферизованные операции для листа default_schedule
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_DEFAULT_SCHEDULE)
            
            if not has_buffered:
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
            else:
                logger.debug(f"Есть буферизованные операции для {SHEET_DEFAULT_SCHEDULE}, пропускаем загрузку из Google Sheets")
        
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
        
        # Сохраняем в файл как JSON
        try:
            with open(DEFAULT_SCHEDULE_FILE, 'w', encoding='utf-8') as f:
                json.dump(schedule, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения расписания по умолчанию в файл: {e}")
    
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
        
        Args:
            places_dict: Dict[str, str] - словарь мест {место: имя}
            
        Returns:
            List[str] - список имен, отсортированный по номеру места
        """
        sorted_places = sorted(places_dict.items(), key=lambda x: (int(x[0].split('.')[0]), int(x[0].split('.')[1])))
        return [name for _, name in sorted_places if name]
    
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
        
        # ПРИОРИТЕТ 2: Локальные файлы (они могут содержать актуальные данные, которые еще не сохранены в PostgreSQL/Google Sheets)
        schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
        if os.path.exists(schedule_file):
            try:
                with open(schedule_file, 'r', encoding='utf-8') as f:
                    current_day = None
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                            current_day = line
                            schedule[current_day] = []
                        elif current_day:
                            employees = [e.strip() for e in line.split(',') if e.strip()]
                            # Если имена уже отформатированы (содержат "(@"), оставляем как есть
                            # Иначе форматируем, если есть employee_manager
                            if employee_manager:
                                formatted_employees = []
                                for emp in employees:
                                    # Проверяем, отформатировано ли уже имя
                                    if '(@' in emp and emp.endswith(')'):
                                        formatted_employees.append(emp)
                                    else:
                                        formatted_employees.append(employee_manager.format_employee_name(emp))
                                schedule[current_day] = formatted_employees
                            else:
                                schedule[current_day] = employees
                if schedule:
                    return schedule
            except Exception as e:
                logger.error(f"Ошибка загрузки расписания на {date_str}: {e}")
        
        # ПРИОРИТЕТ 3: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            # Проверяем, есть ли буферизованные операции для schedules
            # Если есть, приоритет отдаем локальным файлам (которые мы уже проверили выше)
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_SCHEDULES)
            if not has_buffered:
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
                              only_changed_days: bool = False, employee_manager=None):
        """
        Сохранить расписание на неделю в PostgreSQL, Google Sheets и файлы
        
        Args:
            week_start: Начало недели
            schedule: Расписание в формате {day_name: [имена]}
            only_changed_days: Если True, сохранять только дни, отличающиеся от default_schedule
            employee_manager: Менеджер сотрудников для форматирования имен
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
                    # Сохраняем только если отличается от default
                    if is_different:
                        employees_str = ', '.join(employees)
                        try:
                            from database_sync import save_schedule_to_db_sync
                            save_schedule_to_db_sync(date_str, day_name, employees_str)
                            logger.debug(f"Сохранено измененное расписание для {date_str} ({day_name})")
                        except Exception as e:
                            logger.error(f"Ошибка сохранения расписания {date_str} в PostgreSQL: {e}", exc_info=True)
                    else:
                        # Удаляем из schedules, если теперь совпадает с default
                        try:
                            from database_sync import delete_schedule_from_db_sync
                            delete_schedule_from_db_sync(date_str)
                            logger.debug(f"Удалено расписание для {date_str} (совпадает с default)")
                        except Exception as e:
                            logger.debug(f"Не удалось удалить расписание для {date_str}: {e}")
                else:
                    # Сохраняем все дни (старое поведение)
                    employees_str = ', '.join(employees)
                    try:
                        from database_sync import save_schedule_to_db_sync
                        save_schedule_to_db_sync(date_str, day_name, employees_str)
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
                if is_different:
                    # Сохраняем только если отличается
                    schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                    try:
                        with open(schedule_file, 'w', encoding='utf-8') as f:
                            f.write(f"{date_str}\n")
                            f.write(f"{day_name}\n")
                            f.write(f"{', '.join(employees)}\n")
                    except Exception as e:
                        logger.error(f"Ошибка сохранения расписания {date_str} в файл: {e}")
                else:
                    # Удаляем файл, если совпадает с default
                    schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
                    if os.path.exists(schedule_file):
                        try:
                            os.remove(schedule_file)
                            logger.debug(f"Удален файл расписания для {date_str} (совпадает с default)")
                        except Exception as e:
                            logger.debug(f"Не удалось удалить файл расписания для {date_str}: {e}")
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
        
        # Сохраняем обновленное расписание
        schedule[day_name] = employees
        employees_str = ', '.join(employees)
        
        # Сохраняем в PostgreSQL (приоритет 1)
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
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при сохранении расписания {date_str} в PostgreSQL: {e}", exc_info=True)
        else:
            pool = _get_pool()
            logger.warning(f"⚠️ PostgreSQL недоступен для сохранения расписания {date_str}: USE_POSTGRESQL={USE_POSTGRESQL}, _pool={pool is not None}, save_schedule_to_db={save_schedule_to_db is not None}")
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
                return False  # Уже в очереди
        
        # Сохраняем в PostgreSQL (приоритет 1)
        pool = _get_pool()
        logger.info(f"🔄 Начинаю добавление в очередь PostgreSQL: {employee_name} на {date_str}...")
        logger.info(f"   USE_POSTGRESQL={USE_POSTGRESQL}, _pool={pool is not None}, add_to_queue_db={add_to_queue_db is not None}")
        if USE_POSTGRESQL and pool and add_to_queue_db:
            try:
                # Используем синхронную функцию для добавления
                from database_sync import add_to_queue_db_sync
                logger.info(f"   Используем синхронное добавление в очередь PostgreSQL...")
                result = add_to_queue_db_sync(date_str, employee_name, telegram_id)
                logger.info(f"   Получен результат: {result}")
                if result:
                    logger.info(f"✅ Добавлено в очередь PostgreSQL: {employee_name} на {date_str}")
                else:
                    logger.warning(f"⚠️ Не удалось добавить в очередь PostgreSQL: {employee_name} на {date_str}")
            except Exception as e:
                logger.error(f"❌ Ошибка добавления в очередь в PostgreSQL: {e}", exc_info=True)
        else:
            pool = _get_pool()
            logger.warning(f"⚠️ PostgreSQL недоступен для добавления в очередь: USE_POSTGRESQL={USE_POSTGRESQL}, _pool={pool is not None}, add_to_queue_db={add_to_queue_db is not None}")
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
        pool = _get_pool()
        if USE_POSTGRESQL and pool and remove_from_queue_db:
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
        
        # Сохраняем в файл
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        try:
            with open(request_file, 'a', encoding='utf-8') as f:
                f.write(f"{employee_name}:{telegram_id}:{week_str}:{days_req_str}:{days_skip_str}\n")
        except Exception as e:
            logger.error(f"Ошибка сохранения заявки в файл: {e}")
    
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
                                'days_skipped': combined_skipped
                            }
                        else:
                            requests_dict[key] = req
                    
                    if requests_dict:
                        logger.debug(f"Заявки для недели {week_str} загружены из PostgreSQL: {len(requests_dict)} записей")
                        return list(requests_dict.values())
            except Exception as e:
                logger.warning(f"Ошибка загрузки заявок из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        
        # ПРИОРИТЕТ 2: Локальные файлы
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        if os.path.exists(request_file):
            try:
                with open(request_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(':')
                        if len(parts) >= 5:
                            employee_name = parts[0]
                            telegram_id = int(parts[1])
                            week_start_str = parts[2]
                            days_requested = [d for d in parts[3].split(',') if d]
                            days_skipped = [d for d in parts[4].split(',') if d]
                            
                            key = (employee_name, telegram_id)
                            
                            # Если уже есть заявка для этого сотрудника, объединяем
                            if key in requests_dict:
                                existing = requests_dict[key]
                                combined_requested = list(dict.fromkeys(existing['days_requested'] + days_requested))
                                combined_skipped = list(dict.fromkeys(existing['days_skipped'] + days_skipped))
                                combined_requested = [d for d in combined_requested if d not in combined_skipped]
                                
                                requests_dict[key] = {
                                    'employee_name': employee_name,
                                    'telegram_id': telegram_id,
                                    'days_requested': combined_requested,
                                    'days_skipped': combined_skipped
                                }
                            else:
                                requests_dict[key] = {
                                    'employee_name': employee_name,
                                    'telegram_id': telegram_id,
                                    'days_requested': days_requested,
                                    'days_skipped': days_skipped
                                }
            except Exception as e:
                logger.error(f"Ошибка загрузки заявок: {e}")
        
        # ПРИОРИТЕТ 3: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и локальных файлов нет)
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
                                        'days_skipped': combined_skipped
                                    }
                                else:
                                    requests_dict[key] = {
                                        'employee_name': employee_name,
                                        'telegram_id': telegram_id,
                                        'days_requested': days_requested,
                                        'days_skipped': days_skipped
                                    }
                        except (ValueError, IndexError):
                            continue
            except Exception as e:
                logger.warning(f"Ошибка загрузки заявок из Google Sheets: {e}")
        
        return list(requests_dict.values())
    
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
        
        # Удаляем файл
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        if os.path.exists(request_file):
            try:
                os.remove(request_file)
            except Exception as e:
                logger.error(f"Ошибка удаления файла заявок: {e}")
    
    
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
                for day in days_list:
                    if day in schedule:
                        schedule[day][assigned_place] = employee_name
        
        return employee_to_place
    
    def build_schedule_from_requests(self, week_start: datetime, 
                                     requests: List[Dict],
                                     employee_manager) -> Dict[str, List[str]]:
        """
        Построить расписание на основе заявок с сохранением фиксированных мест
        
        Returns:
            Dict[str, List[str]] - расписание в формате {день: [имена]} для обратной совместимости
        """
        # Начинаем с расписания по умолчанию (в новом формате JSON)
        default_schedule = self.load_default_schedule()
        
        # Копируем расписание по умолчанию (но очищаем имена, оставляя только структуру мест)
        # Всегда создаем все 8 мест для каждого дня, даже если в default_schedule их меньше
        schedule = {}
        for day_name, places_dict in default_schedule.items():
            schedule[day_name] = {}
            # Копируем структуру мест из default_schedule
            for place_key in places_dict.keys():
                schedule[day_name][place_key] = ''
            # Дополняем до MAX_OFFICE_SEATS, если мест меньше
            for i in range(1, MAX_OFFICE_SEATS + 1):
                place_key = f'1.{i}'
                if place_key not in schedule[day_name]:
                    schedule[day_name][place_key] = ''
        
        # Шаг 1: Назначаем фиксированные места сотрудникам на основе приоритета
        employee_to_place = self._assign_fixed_places(default_schedule, schedule, employee_manager)
        
        # Шаг 2: Применяем заявки (skip_day, add_day)
        # Создаем словарь заявок по сотрудникам
        requests_by_employee = {}
        for req in requests:
            employee_name = req['employee_name']
            requests_by_employee[employee_name] = {
                'days_requested': req['days_requested'],
                'days_skipped': req['days_skipped']
            }
        
        # Обрабатываем заявки
        for employee_name, req_info in requests_by_employee.items():
            days_requested = req_info['days_requested']
            days_skipped = req_info['days_skipped']
            
            # Получаем фиксированное место сотрудника (если есть)
            fixed_place = employee_to_place.get(employee_name)
            
            # Удаляем сотрудника из пропущенных дней
            for day in days_skipped:
                if day in schedule and fixed_place:
                    # Освобождаем место
                    if fixed_place in schedule[day]:
                        schedule[day][fixed_place] = ''
            
            # Добавляем сотрудника в запрошенные дни (которые не в пропусках)
            for day in days_requested:
                if day in schedule and day not in days_skipped:
                    # Проверяем, есть ли уже сотрудник в расписании
                    place_key = self._find_employee_in_places(schedule[day], employee_name)
                    if not place_key:
                        # Если у сотрудника есть фиксированное место, используем его
                        if fixed_place:
                            # Проверяем, свободно ли место
                            if fixed_place not in schedule[day] or not schedule[day].get(fixed_place):
                                schedule[day][fixed_place] = employee_name
                            else:
                                # Место занято - ищем свободное
                                free_place = self._find_free_place(schedule[day], department=1)
                                if free_place:
                                    schedule[day][free_place] = employee_name
                        else:
                            # У сотрудника нет фиксированного места - ищем свободное
                            free_place = self._find_free_place(schedule[day], department=1)
                            if free_place:
                                schedule[day][free_place] = employee_name
                        # Если места нет, сотрудник не добавляется (работает удаленно)
        
        # Конвертируем обратно в формат списка для вывода
        formatted_schedule = {}
        for day, places_dict in schedule.items():
            employees = self._get_employees_list_from_places(places_dict)
            # Форматируем имена с никнеймами для вывода
            formatted_schedule[day] = [employee_manager.format_employee_name(emp) for emp in employees]
        
        return formatted_schedule
    
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

