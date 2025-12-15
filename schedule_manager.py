"""
Управление расписаниями
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import (
    SCHEDULES_DIR, REQUESTS_DIR, QUEUE_DIR, DEFAULT_SCHEDULE_FILE, 
    DEFAULT_SCHEDULE, MAX_OFFICE_SEATS, DATA_DIR
)
import pytz
from config import TIMEZONE


class ScheduleManager:
    """Класс для управления расписаниями"""
    
    def __init__(self, employee_manager=None):
        self.timezone = pytz.timezone(TIMEZONE)
        self.employee_manager = employee_manager
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
    
    def load_default_schedule(self) -> Dict[str, List[str]]:
        """Загрузить расписание по умолчанию"""
        schedule = {}
        if os.path.exists(DEFAULT_SCHEDULE_FILE):
            try:
                with open(DEFAULT_SCHEDULE_FILE, 'r', encoding='utf-8') as f:
                    current_day = None
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                            current_day = line
                            schedule[current_day] = []
                        elif current_day:
                            employees = [e.strip() for e in line.split(',')]
                            schedule[current_day] = employees
            except Exception as e:
                print(f"Ошибка загрузки расписания по умолчанию: {e}")
        
        # Если не загрузилось, используем из config
        if not schedule:
            schedule = DEFAULT_SCHEDULE.copy()
        
        return schedule
    
    def get_plain_name_from_formatted(self, formatted_name: str) -> str:
        """Извлечь простое имя из отформатированного (например, 'Рома(@rsidorenkov)' -> 'Рома')"""
        if '(@' in formatted_name and formatted_name.endswith(')'):
            return formatted_name.split('(@')[0]
        return formatted_name
    
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
    
    def load_schedule_for_date(self, date: datetime, employee_manager=None) -> Dict[str, List[str]]:
        """Загрузить расписание на конкретную дату"""
        date_str = date.strftime('%Y-%m-%d')
        schedule_file = os.path.join(SCHEDULES_DIR, f"{date_str}.txt")
        
        if os.path.exists(schedule_file):
            schedule = {}
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
                return schedule
            except Exception as e:
                print(f"Ошибка загрузки расписания на {date_str}: {e}")
        
        # Если файла нет, возвращаем расписание по умолчанию
        default_schedule = self.load_default_schedule()
        # Форматируем имена в расписании по умолчанию, если есть employee_manager
        if employee_manager:
            formatted_default = {}
            for day, employees in default_schedule.items():
                formatted_default[day] = [employee_manager.format_employee_name(emp) for emp in employees]
            return formatted_default
        return default_schedule
    
    def save_schedule_for_week(self, week_start: datetime, schedule: Dict[str, List[str]]):
        """Сохранить расписание на неделю"""
        week_dates = self.get_week_dates(week_start)
        
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
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        
        # Проверяем, не в очереди ли уже
        queue = self.get_queue_for_date(date)
        for entry in queue:
            if entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id:
                return False  # Уже в очереди
        
        # Добавляем в очередь
        with open(queue_file, 'a', encoding='utf-8') as f:
            f.write(f"{employee_name}:{telegram_id}\n")
        return True
    
    def get_queue_for_date(self, date: datetime) -> List[Dict]:
        """Получить очередь на дату"""
        date_str = date.strftime('%Y-%m-%d')
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        
        queue = []
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
                print(f"Ошибка загрузки очереди: {e}")
        
        return queue
    
    def remove_from_queue(self, date: datetime, employee_name: str, telegram_id: int):
        """Удалить сотрудника из очереди на дату"""
        date_str = date.strftime('%Y-%m-%d')
        queue_file = os.path.join(QUEUE_DIR, f"{date_str}_queue.txt")
        
        if not os.path.exists(queue_file):
            return
        
        queue = self.get_queue_for_date(date)
        # Удаляем сотрудника из очереди
        queue = [entry for entry in queue 
                if not (entry['employee_name'] == employee_name and entry['telegram_id'] == telegram_id)]
        
        # Сохраняем обновленную очередь
        with open(queue_file, 'w', encoding='utf-8') as f:
            for entry in queue:
                f.write(f"{entry['employee_name']}:{entry['telegram_id']}\n")
    
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
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        
        # Удаляем дубликаты
        days_requested = list(dict.fromkeys(days_requested))  # Сохраняет порядок
        days_skipped = list(dict.fromkeys(days_skipped))
        
        with open(request_file, 'a', encoding='utf-8') as f:
            days_req_str = ','.join(days_requested) if days_requested else ''
            days_skip_str = ','.join(days_skipped) if days_skipped else ''
            f.write(f"{employee_name}:{telegram_id}:{week_str}:{days_req_str}:{days_skip_str}\n")
    
    def load_requests_for_week(self, week_start: datetime) -> List[Dict]:
        """Загрузить все заявки на неделю (схлопывает дубликаты для одного сотрудника)"""
        week_str = week_start.strftime('%Y-%m-%d')
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        
        requests_dict = {}  # Ключ: (employee_name, telegram_id), значение: заявка
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
            print(f"Ошибка загрузки заявок: {e}")
        
        return list(requests_dict.values())
    
    def clear_requests_for_week(self, week_start: datetime):
        """Очистить заявки на неделю (после формирования расписания)"""
        week_str = week_start.strftime('%Y-%m-%d')
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        if os.path.exists(request_file):
            os.remove(request_file)
    
    def build_schedule_from_requests(self, week_start: datetime, 
                                     requests: List[Dict],
                                     employee_manager) -> Dict[str, List[str]]:
        """Построить расписание на основе заявок"""
        # Начинаем с расписания по умолчанию
        schedule = self.load_default_schedule()
        
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
                    for i in range(len(schedule[day]) - 1, -1, -1):  # Идем с конца, чтобы не сломать индексы
                        emp = schedule[day][i]
                        plain_name = self.get_plain_name_from_formatted(emp)
                        if plain_name == employee_name:
                            schedule[day].pop(i)
                            break
            
            # Добавляем сотрудника в запрошенные дни (которые не в пропусках)
            for day in days_requested:
                if day in schedule and day not in days_skipped:
                    # Проверяем, есть ли уже сотрудник в списке (может быть отформатированным)
                    employee_exists = False
                    for emp in schedule[day]:
                        plain_name = self.get_plain_name_from_formatted(emp)
                        if plain_name == employee_name:
                            employee_exists = True
                            break
                    if not employee_exists:
                        # Проверяем, есть ли место
                        if len(schedule[day]) < MAX_OFFICE_SEATS:
                            schedule[day].append(employee_name)
                        # Если места нет, сотрудник не добавляется (работает удаленно)
        
        # Форматируем имена с никнеймами для вывода
        formatted_schedule = {}
        for day, employees in schedule.items():
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
        """Обновить имя сотрудника в default_schedule.txt (заменить простое имя на форматированное)"""
        if not os.path.exists(DEFAULT_SCHEDULE_FILE):
            return
        
        try:
            # Читаем файл
            with open(DEFAULT_SCHEDULE_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Обновляем строки
            updated_lines = []
            for line in lines:
                line = line.rstrip('\n')
                # Если это строка с именами сотрудников
                if line and line not in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                    # Разбиваем на имена
                    employees = [e.strip() for e in line.split(',')]
                    updated_employees = []
                    for emp in employees:
                        # Извлекаем простое имя из отформатированного (если есть)
                        plain_name = self.get_plain_name_from_formatted(emp)
                        # Если простое имя совпадает с old_name, заменяем на новое форматированное
                        if plain_name == old_name:
                            updated_employees.append(new_formatted_name)
                        else:
                            updated_employees.append(emp)
                    updated_lines.append(', '.join(updated_employees) + '\n')
                else:
                    updated_lines.append(line + '\n')
            
            # Записываем обратно
            with open(DEFAULT_SCHEDULE_FILE, 'w', encoding='utf-8') as f:
                f.writelines(updated_lines)
        except Exception as e:
            print(f"Ошибка обновления имени в default_schedule.txt: {e}")
    
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

