"""
Управление расписаниями
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from config import (
    SCHEDULES_DIR, REQUESTS_DIR, DEFAULT_SCHEDULE_FILE, 
    DEFAULT_SCHEDULE, MAX_OFFICE_SEATS, DATA_DIR
)
import pytz
from config import TIMEZONE


class ScheduleManager:
    """Класс для управления расписаниями"""
    
    def __init__(self):
        self.timezone = pytz.timezone(TIMEZONE)
        self._ensure_directories()
        self._save_default_schedule()
    
    def _ensure_directories(self):
        """Создать необходимые директории"""
        os.makedirs(SCHEDULES_DIR, exist_ok=True)
        os.makedirs(REQUESTS_DIR, exist_ok=True)
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
    
    def load_schedule_for_date(self, date: datetime) -> Dict[str, List[str]]:
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
                            schedule[current_day] = employees
                return schedule
            except Exception as e:
                print(f"Ошибка загрузки расписания на {date_str}: {e}")
        
        # Если файла нет, возвращаем расписание по умолчанию
        return self.load_default_schedule()
    
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
    
    def save_request(self, employee_name: str, telegram_id: int, week_start: datetime,
                    days_requested: List[str], days_skipped: List[str]):
        """Сохранить заявку сотрудника"""
        week_str = week_start.strftime('%Y-%m-%d')
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        
        with open(request_file, 'a', encoding='utf-8') as f:
            days_req_str = ','.join(days_requested)
            days_skip_str = ','.join(days_skipped)
            f.write(f"{employee_name}:{telegram_id}:{week_str}:{days_req_str}:{days_skip_str}\n")
    
    def load_requests_for_week(self, week_start: datetime) -> List[Dict]:
        """Загрузить все заявки на неделю"""
        week_str = week_start.strftime('%Y-%m-%d')
        request_file = os.path.join(REQUESTS_DIR, f"{week_str}_requests.txt")
        
        requests = []
        if not os.path.exists(request_file):
            return requests
        
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
                        
                        requests.append({
                            'employee_name': employee_name,
                            'telegram_id': telegram_id,
                            'days_requested': days_requested,
                            'days_skipped': days_skipped
                        })
        except Exception as e:
            print(f"Ошибка загрузки заявок: {e}")
        
        return requests
    
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
        
        # Применяем пропуски
        for req in requests:
            employee_name = req['employee_name']
            for day in req['days_skipped']:
                if day in schedule and employee_name in schedule[day]:
                    schedule[day].remove(employee_name)
        
        # Применяем дополнительные заявки
        # Сортируем по времени заявки (приоритет)
        for req in requests:
            employee_name = req['employee_name']
            for day in req['days_requested']:
                if day not in schedule:
                    continue
                if employee_name not in schedule[day]:
                    # Проверяем, есть ли место
                    if len(schedule[day]) < MAX_OFFICE_SEATS:
                        schedule[day].append(employee_name)
                    # Если места нет, сотрудник не добавляется (работает удаленно)
        
        return schedule
    
    def get_available_slots(self, schedule: Dict[str, List[str]]) -> Dict[str, int]:
        """Получить количество свободных мест по дням"""
        available = {}
        for day, employees in schedule.items():
            available[day] = MAX_OFFICE_SEATS - len(employees)
        return available
    
    def get_employee_schedule(self, week_start: datetime, employee_name: str) -> Dict[str, bool]:
        """Получить расписание сотрудника на неделю (True - в офисе, False - удаленно)"""
        schedule = self.load_schedule_for_date(week_start)
        week_dates = self.get_week_dates(week_start)
        
        employee_schedule = {}
        for date, day_name in week_dates:
            employees = schedule.get(day_name, [])
            employee_schedule[day_name] = employee_name in employees
        
        return employee_schedule

