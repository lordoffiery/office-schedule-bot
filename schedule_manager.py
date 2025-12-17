"""
Управление расписаниями
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import (
    SCHEDULES_DIR, REQUESTS_DIR, QUEUE_DIR, DEFAULT_SCHEDULE_FILE, 
    DEFAULT_SCHEDULE, MAX_OFFICE_SEATS, DATA_DIR,
    USE_GOOGLE_SHEETS, SHEET_REQUESTS, SHEET_SCHEDULES, SHEET_QUEUE, SHEET_DEFAULT_SCHEDULE
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
        Загрузить расписание по умолчанию в формате JSON (день -> {подразделение.место: имя})
        
        Returns:
            Dict[str, Dict[str, str]]: Расписание по дням, где внутренний словарь - места (ключ: "подразделение.место")
        """
        schedule = {}
        
        # Пробуем загрузить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
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
        
        # Если не загрузилось, используем из config
        if not schedule:
            schedule = DEFAULT_SCHEDULE.copy()
        
        return schedule
    
    def save_default_schedule(self, schedule: Dict[str, Dict[str, str]]):
        """
        Сохранить расписание по умолчанию в Google Sheets и файл (JSON формат)
        
        Args:
            schedule: Dict[str, Dict[str, str]] - расписание по дням, где внутренний словарь - места (ключ: "подразделение.место")
        """
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows_to_save = [['day_name', 'places_json']]  # Заголовок
                for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                    places_dict = schedule.get(day_name, {})
                    # Сохраняем как JSON строку
                    places_json = json.dumps(places_dict, ensure_ascii=False)
                    rows_to_save.append([day_name, places_json])
                
                # Перезаписываем весь лист
                self.sheets_manager.write_rows(SHEET_DEFAULT_SCHEDULE, rows_to_save, clear_first=True)
                logger.info(f"Расписание по умолчанию сохранено в Google Sheets")
            except Exception as e:
                logger.error(f"Ошибка сохранения расписания по умолчанию в Google Sheets: {e}, используем файлы")
        
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
        (проверяет и локальные файлы, и Google Sheets)
        
        Args:
            week_start: Начало недели
            
        Returns:
            True если есть сохраненные расписания, False иначе
        """
        week_dates = self.get_week_dates(week_start)
        
        # Сначала проверяем локальные файлы
        for d, day_name in week_dates:
            date_str = d.strftime('%Y-%m-%d')
            schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
            if os.path.exists(schedule_file):
                return True
        
        # Если локальных файлов нет, проверяем Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                week_dates_str = [d.strftime('%Y-%m-%d') for d, _ in week_dates]
                for row in rows[start_idx:]:
                    if len(row) >= 1 and row[0] and row[0].strip() in week_dates_str:
                        return True
            except Exception as e:
                logger.warning(f"Ошибка проверки расписаний в Google Sheets: {e}")
        
        return False
    
    def load_schedule_for_date(self, date: datetime, employee_manager=None) -> Dict[str, List[str]]:
        """Загрузить расписание на конкретную дату"""
        date_str = date.strftime('%Y-%m-%d')
        schedule = {}
        
        # Пробуем загрузить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_SCHEDULES)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[0] and row[0].strip() == date_str:
                        try:
                            day_name = row[1].strip()
                            employees_str = row[2].strip() if row[2] else ""
                            employees = [e.strip() for e in employees_str.split(',') if e.strip()]
                            
                            # Форматируем имена, если нужно
                            if employee_manager:
                                formatted_employees = []
                                for emp in employees:
                                    if '(@' in emp and emp.endswith(')'):
                                        formatted_employees.append(emp)
                                    else:
                                        formatted_employees.append(employee_manager.format_employee_name(emp))
                                schedule[day_name] = formatted_employees
                            else:
                                schedule[day_name] = employees
                        except (ValueError, IndexError):
                            continue
                # Если загрузили из Google Sheets, возвращаем результат
                if schedule:
                    return schedule
            except Exception as e:
                logger.warning(f"Ошибка загрузки расписания из Google Sheets: {e}, используем файлы")
        
        # Загружаем из файла
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
        
        # Если файла нет, возвращаем расписание по умолчанию
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
    
    def save_schedule_for_week(self, week_start: datetime, schedule: Dict[str, List[str]]):
        """Сохранить расписание на неделю"""
        week_dates = self.get_week_dates(week_start)
        
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows_to_save = []
                for date, day_name in week_dates:
                    date_str = date.strftime('%Y-%m-%d')
                    employees = schedule.get(day_name, [])
                    employees_str = ', '.join(employees)
                    rows_to_save.append([date_str, day_name, employees_str])
                
                # Обновляем записи для этой недели
                worksheet = self.sheets_manager.get_worksheet(SHEET_SCHEDULES)
                if worksheet:
                    all_rows = worksheet.get_all_values()
                    all_rows = filter_empty_rows(all_rows)
                    
                    # Получаем даты недели
                    week_dates_str = [d.strftime('%Y-%m-%d') for d, _ in week_dates]
                    
                    # Пропускаем заголовок, если есть
                    start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
                    rows_to_keep = [all_rows[0]] if has_header else [['date', 'day_name', 'employees']]
                    
                    # Оставляем только записи не для этой недели
                    for row in all_rows[start_idx:]:
                        if len(row) >= 1 and row[0] and row[0].strip() not in week_dates_str:
                            rows_to_keep.append(row)
                    # Добавляем новые записи для этой недели
                    rows_to_keep.extend(rows_to_save)
                    # Перезаписываем весь лист
                    self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_keep, clear_first=True)
            except Exception as e:
                logger.error(f"Ошибка сохранения расписания недели в Google Sheets: {e}, используем файлы")
        
        # Сохраняем в файлы
        for date, day_name in week_dates:
            date_str = date.strftime('%Y-%m-%d')
            schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
            
            employees = schedule.get(day_name, [])
            
            with open(schedule_file, 'w', encoding='utf-8') as f:
                f.write(f"{date_str}\n")
                f.write(f"{day_name}\n")
                f.write(f"{', '.join(employees)}\n")
    
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
        
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                logger.info(f"Сохранение расписания в Google Sheets для {date_str}, день: {day_name}")
                # Сохраняем только измененный день (как в файле)
                employees_str = ', '.join(employees)
                row = [date_str, day_name, employees_str]
                
                # Обновляем записи в Google Sheets
                worksheet = self.sheets_manager.get_worksheet(SHEET_SCHEDULES)
                if worksheet:
                    all_rows = worksheet.get_all_values()
                    all_rows = filter_empty_rows(all_rows)
                    
                    # Пропускаем заголовок, если есть
                    start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
                    rows_to_keep = [all_rows[0]] if has_header else [['date', 'day_name', 'employees']]
                    
                    # Оставляем только записи не для этой даты и дня
                    found = False
                    for row_data in all_rows[start_idx:]:
                        if len(row_data) >= 2 and row_data[0] and row_data[0].strip() == date_str and row_data[1] and row_data[1].strip() == day_name:
                            # Это запись для этой даты и дня - заменяем её
                            found = True
                            logger.info(f"Найдена существующая запись для {date_str} {day_name}, заменяю")
                            rows_to_keep.append(row)
                        elif len(row_data) >= 1 and row_data[0] != date_str:
                            # Запись для другой даты - оставляем
                            rows_to_keep.append(row_data)
                    
                    # Если не нашли существующую запись, добавляем новую
                    if not found:
                        logger.info(f"Не найдена существующая запись для {date_str} {day_name}, добавляю новую")
                        rows_to_keep.append(row)
                    
                    # Перезаписываем весь лист
                    logger.info(f"Сохраняю {len(rows_to_keep)} строк в Google Sheets (включая заголовок)")
                    logger.info(f"Данные для сохранения: date={date_str}, day={day_name}, employees={employees_str[:100]}")
                    self.sheets_manager.write_rows(SHEET_SCHEDULES, rows_to_keep, clear_first=True)
                    logger.info(f"✅ Расписание успешно сохранено в Google Sheets для {date_str}")
                else:
                    logger.warning(f"Не удалось получить лист {SHEET_SCHEDULES}")
            except Exception as e:
                logger.error(f"Ошибка сохранения расписания в Google Sheets: {e}", exc_info=True)
        else:
            logger.warning(f"Google Sheets не доступен (sheets_manager={self.sheets_manager}, is_available={self.sheets_manager.is_available() if self.sheets_manager else False})")
        
        # Сохраняем в файл
        with open(schedule_file, 'w', encoding='utf-8') as f:
            f.write(f"{date_str}\n")
            f.write(f"{day_name}\n")
            f.write(f"{', '.join(employees)}\n")
        
        # Возвращаем количество свободных мест
        free_slots = MAX_OFFICE_SEATS - len(employees)
        return True, free_slots
    
    def add_to_queue(self, date: datetime, employee_name: str, telegram_id: int):
        """Добавить сотрудника в очередь на дату"""
        date_str = date.strftime('%Y-%m-%d')
        
        # Проверяем, не в очереди ли уже
        queue = self.get_queue_for_date(date)
        for entry in queue:
            if entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id:
                return False  # Уже в очереди
        
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                row = [date_str, employee_name, str(telegram_id)]
                self.sheets_manager.append_row(SHEET_QUEUE, row)
            except Exception as e:
                logger.warning(f"Ошибка сохранения в очередь в Google Sheets: {e}, используем файлы")
        
        # Добавляем в очередь (файл)
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        with open(queue_file, 'a', encoding='utf-8') as f:
            f.write(f"{employee_name}:{telegram_id}\n")
        return True
    
    def get_queue_for_date(self, date: datetime) -> List[Dict]:
        """Получить очередь на дату"""
        date_str = date.strftime('%Y-%m-%d')
        queue = []
        
        # Пробуем загрузить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_QUEUE)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата'])
                for row in rows[start_idx:]:
                    if len(row) >= 3 and row[0] == date_str:
                        try:
                            employee_name = row[1].strip()
                            telegram_id = int(row[2].strip())
                            queue.append({
                                'employee_name': employee_name,
                                'telegram_id': telegram_id
                            })
                        except (ValueError, IndexError):
                            continue
                # Если загрузили из Google Sheets (даже если очередь пуста), возвращаем результат
                # Проверяем, есть ли вообще записи для этой даты в Google Sheets
                # Если есть хотя бы одна запись (даже не для этой даты), значит Google Sheets используется
                if rows and len(rows) > start_idx:
                    # В Google Sheets есть данные, возвращаем очередь (даже если пуста для этой даты)
                    return queue
            except Exception as e:
                logger.warning(f"Ошибка загрузки очереди из Google Sheets: {e}, используем файлы")
        
        # Загружаем из файла
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
        
        return queue
    
    def remove_from_queue(self, date: datetime, employee_name: str, telegram_id: int):
        """Удалить сотрудника из очереди на дату"""
        date_str = date.strftime('%Y-%m-%d')
        
        logger.info(f"Удаление из очереди: {date_str}, сотрудник: {employee_name}, ID: {telegram_id}")
        
        queue = self.get_queue_for_date(date)
        logger.info(f"Очередь до удаления: {len(queue)} записей")
        
        # Удаляем сотрудника из очереди
        queue = [entry for entry in queue 
                if not (entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id)]
        
        logger.info(f"Очередь после удаления: {len(queue)} записей")
        
        # Пробуем обновить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                # Удаляем все записи для этой даты и добавляем обновленные
                worksheet = self.sheets_manager.get_worksheet(SHEET_QUEUE)
                if worksheet:
                    all_rows = worksheet.get_all_values()
                    logger.info(f"Всего строк в Google Sheets: {len(all_rows)}")
                    
                    all_rows = filter_empty_rows(all_rows)
                    
                    # Пропускаем заголовок
                    start_idx, has_header = get_header_start_idx(all_rows, ['date', 'date_str', 'Дата'])
                    rows_to_keep = [all_rows[0]] if has_header else [['date', 'employee_name', 'telegram_id']]
                    
                    # Оставляем только записи не для этой даты
                    for row in all_rows[start_idx:]:
                        if len(row) >= 1 and row[0] != date_str:
                            rows_to_keep.append(row)
                    
                    # Добавляем обновленные записи для этой даты (если очередь не пуста)
                    for entry in queue:
                        rows_to_keep.append([date_str, entry['employee_name'], str(entry['telegram_id'])])
                    
                    logger.info(f"Сохраняю {len(rows_to_keep)} строк в Google Sheets (включая заголовок)")
                    # Перезаписываем весь лист (даже если очередь пуста - это удалит запись)
                    self.sheets_manager.write_rows(SHEET_QUEUE, rows_to_keep, clear_first=True)
                    logger.info(f"Очередь обновлена в Google Sheets")
            except Exception as e:
                logger.error(f"Ошибка обновления очереди в Google Sheets: {e}", exc_info=True)
        else:
            logger.warning(f"Google Sheets не доступен для обновления очереди")
        
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
        """Сохранить заявку сотрудника"""
        week_str = week_start.strftime('%Y-%m-%d')
        
        # Удаляем дубликаты
        days_requested = list(dict.fromkeys(days_requested))  # Сохраняет порядок
        days_skipped = list(dict.fromkeys(days_skipped))
        
        days_req_str = ','.join(days_requested) if days_requested else ''
        days_skip_str = ','.join(days_skipped) if days_skipped else ''
        
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                # Проверяем, есть ли заголовок, если нет - добавляем
                worksheet = self.sheets_manager.get_worksheet(SHEET_REQUESTS)
                if worksheet:
                    all_rows = worksheet.get_all_values()
                    all_rows = filter_empty_rows(all_rows)
                    
                    # Проверяем, есть ли заголовок
                    _, has_header = get_header_start_idx(all_rows, ['week_start', 'week', 'Неделя', 'employee_name'])
                    
                    # Если заголовка нет, добавляем его
                    if not has_header:
                        header = ['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped']
                        self.sheets_manager.write_rows(SHEET_REQUESTS, [header], clear_first=True)
                        logger.info(f"Добавлен заголовок в лист {SHEET_REQUESTS}")
                
                # Формируем строку для таблицы: [week_start, employee_name, telegram_id, days_requested, days_skipped]
                row = [week_str, employee_name, str(telegram_id), days_req_str, days_skip_str]
                self.sheets_manager.append_row(SHEET_REQUESTS, row)
            except Exception as e:
                logger.warning(f"Ошибка сохранения заявки в Google Sheets: {e}, используем файлы")
        
        # Сохраняем в файл
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        with open(request_file, 'a', encoding='utf-8') as f:
            f.write(f"{employee_name}:{telegram_id}:{week_str}:{days_req_str}:{days_skip_str}\n")
    
    def load_requests_for_week(self, week_start: datetime) -> List[Dict]:
        """Загрузить все заявки на неделю (схлопывает дубликаты для одного сотрудника)"""
        week_str = week_start.strftime('%Y-%m-%d')
        requests_dict = {}  # Ключ: (employee_name, telegram_id), значение: заявка
        
        # Пробуем загрузить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_REQUESTS)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['week_start', 'week', 'Неделя', 'employee_name'])
                for row in rows[start_idx:]:
                    if len(row) < 5 or not row[0] or row[0] != week_str:
                        continue
                    try:
                        employee_name = row[1].strip()
                        telegram_id = int(row[2].strip())
                        days_requested = [d.strip() for d in row[3].split(',') if d.strip()] if row[3] else []
                        days_skipped = [d.strip() for d in row[4].split(',') if d.strip()] if row[4] else []
                        
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
                    except (ValueError, IndexError):
                        continue
                # Если загрузили из Google Sheets (даже если заявок нет для этой недели), возвращаем результат
                # Проверяем, есть ли вообще записи в Google Sheets
                # Если есть хотя бы одна запись (даже не для этой недели), значит Google Sheets используется
                if rows and len(rows) > start_idx:
                    # В Google Sheets есть данные, возвращаем заявки (даже если пусто для этой недели)
                    return list(requests_dict.values())
            except Exception as e:
                logger.warning(f"Ошибка загрузки заявок из Google Sheets: {e}, используем файлы")
        
        # Загружаем из файла
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        if not os.path.exists(request_file):
            return []
        
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
                            # Объединяем запрошенные дни (убираем дубликаты)
                            combined_requested = list(dict.fromkeys(existing['days_requested'] + days_requested))
                            # Объединяем пропущенные дни (убираем дубликаты)
                            combined_skipped = list(dict.fromkeys(existing['days_skipped'] + days_skipped))
                            # Удаляем из запрошенных те дни, которые есть в пропущенных
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
        
        return list(requests_dict.values())
    
    def clear_requests_for_week(self, week_start: datetime):
        """Очистить заявки на неделю (после формирования расписания)"""
        week_str = week_start.strftime('%Y-%m-%d')
        
        # Пробуем удалить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                worksheet = self.sheets_manager.get_worksheet(SHEET_REQUESTS)
                if worksheet:
                    all_rows = worksheet.get_all_values()
                    all_rows = filter_empty_rows(all_rows)
                    
                    # Пропускаем заголовок
                    start_idx, has_header = get_header_start_idx(all_rows, ['week_start', 'week', 'Неделя', 'employee_name'])
                    rows_to_keep = [all_rows[0]] if has_header else [['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped']]
                    
                    # Оставляем только записи не для этой недели
                    for row in all_rows[start_idx:]:
                        if len(row) >= 1 and row[0] and row[0].strip() != week_str:
                            rows_to_keep.append(row)
                    # Перезаписываем весь лист
                    self.sheets_manager.write_rows(SHEET_REQUESTS, rows_to_keep, clear_first=True)
            except Exception as e:
                logger.warning(f"Ошибка очистки заявок в Google Sheets: {e}, используем файлы")
        
        # Удаляем файл
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        if os.path.exists(request_file):
            os.remove(request_file)
    
    def build_schedule_from_requests(self, week_start: datetime, 
                                     requests: List[Dict],
                                     employee_manager) -> Dict[str, List[str]]:
        """
        Построить расписание на основе заявок
        
        Returns:
            Dict[str, List[str]] - расписание в формате {день: [имена]} для обратной совместимости
        """
        # Начинаем с расписания по умолчанию (в новом формате JSON)
        default_schedule = self.load_default_schedule()
        
        # Конвертируем в рабочий формат (копируем, чтобы не изменять оригинал)
        schedule = {}
        for day_name, places_dict in default_schedule.items():
            schedule[day_name] = places_dict.copy()
        
        # Для каждого сотрудника с заявкой:
        # 1. Удаляем его из дней, которые он пропустил
        # 2. Добавляем его в дни, которые он запросил дополнительно (если есть место)
        for req in requests:
            employee_name = req['employee_name']
            days_requested = req['days_requested']
            days_skipped = req['days_skipped']
            
            # Удаляем сотрудника из пропущенных дней
            for day in days_skipped:
                if day in schedule:
                    place_key = self._find_employee_in_places(schedule[day], employee_name)
                    if place_key:
                        # Освобождаем место
                        schedule[day][place_key] = ''
            
            # Добавляем сотрудника в запрошенные дни (которые не в пропусках)
            for day in days_requested:
                if day in schedule and day not in days_skipped:
                    # Проверяем, есть ли уже сотрудник в расписании
                    place_key = self._find_employee_in_places(schedule[day], employee_name)
                    if not place_key:
                        # Ищем свободное место (начинаем с первого подразделения)
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

