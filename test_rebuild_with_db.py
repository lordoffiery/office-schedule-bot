#!/usr/bin/env python3
"""
Тестовый скрипт для воспроизведения команды /admin_rebuild_schedules_from_requests
с реальными данными из PostgreSQL
"""
import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List
import pytz

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Пытаемся загрузить переменные окружения из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv не установлен, используем системные переменные окружения

# Устанавливаем минимальные переменные окружения для тестирования
if not os.getenv('BOT_TOKEN'):
    os.environ['BOT_TOKEN'] = 'test_token'

# Проверяем наличие DATABASE_URL
if not os.getenv('DATABASE_URL') and not os.getenv('DATABASE_PUBLIC_URL'):
    # Пытаемся получить из аргументов командной строки
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
        print(f"✅ Используется DATABASE_URL из аргументов командной строки")
    else:
        print("❌ Ошибка: DATABASE_URL не установлен в переменных окружения")
        print("Использование: python3 test_rebuild_with_db.py <DATABASE_URL>")
        print("Или установите переменную окружения DATABASE_URL")
        sys.exit(1)

from config import TIMEZONE, DATABASE_URL
from employee_manager import EmployeeManager
from schedule_manager import ScheduleManager
from database_sync import (
    load_default_schedule_from_db_sync,
    load_requests_from_db_sync,
    load_schedule_from_db_sync
)

def get_week_start(date: datetime) -> datetime:
    """Получить начало недели (понедельник) для даты"""
    days_since_monday = date.weekday()
    week_start = date - timedelta(days=days_since_monday)
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0)

def get_week_dates(week_start: datetime) -> List[tuple]:
    """Получить все даты недели (пн-пт)"""
    week_dates = []
    weekdays_ru = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']
    for i, day_name in enumerate(weekdays_ru):
        date = week_start + timedelta(days=i)
        week_dates.append((date, day_name))
    return week_dates

def main():
    print("="*60)
    print("ТЕСТ КОМАНДЫ /admin_rebuild_schedules_from_requests")
    print("="*60)
    
    # Инициализируем менеджеры (они автоматически загружают данные при инициализации)
    employee_manager = EmployeeManager()
    schedule_manager = ScheduleManager()
    
    # Сотрудники загружаются автоматически при инициализации
    employees_count = len(employee_manager.employees) if hasattr(employee_manager, 'employees') else 0
    print(f"\n✅ Загружено сотрудников: {employees_count}")
    
    # Дата для тестирования
    timezone = pytz.timezone(TIMEZONE)
    test_date = datetime(2025, 12, 22, tzinfo=timezone)
    week_start = get_week_start(test_date)
    week_str = week_start.strftime('%Y-%m-%d')
    
    print(f"\n📅 Тестируем неделю: {week_str} (начало: {week_start.strftime('%Y-%m-%d %H:%M:%S')})")
    
    # Загружаем default_schedule из PostgreSQL
    print("\n📋 Загружаем default_schedule из PostgreSQL...")
    print(f"   DATABASE_URL: {DATABASE_URL[:50] if DATABASE_URL else 'не установлен'}...")
    default_schedule_db = load_default_schedule_from_db_sync()
    if default_schedule_db:
        print(f"✅ Загружено {len(default_schedule_db)} дней из default_schedule")
        for day, places in default_schedule_db.items():
            employees_count = len([name for name in places.values() if name])
            print(f"  {day}: {employees_count} сотрудников")
    else:
        print("❌ Не удалось загрузить default_schedule из PostgreSQL")
        print("   Проверьте подключение к базе данных и наличие данных в таблице default_schedule")
        return
    
    # Загружаем requests из PostgreSQL
    print(f"\n📋 Загружаем requests для недели {week_str} из PostgreSQL...")
    requests = load_requests_from_db_sync(week_str)
    if requests:
        print(f"✅ Загружено {len(requests)} заявок:")
        for req in requests:
            print(f"  - {req['employee_name']}: запрошены дни {req['days_requested']}, пропущены дни {req['days_skipped']}")
    else:
        print("⚠️ Нет заявок для этой недели")
        requests = []
    
    # Загружаем текущие schedules из PostgreSQL
    print(f"\n📋 Загружаем текущие schedules для недели {week_str} из PostgreSQL...")
    week_dates = get_week_dates(week_start)
    current_schedules = {}
    for date, day_name in week_dates:
        date_str = date.strftime('%Y-%m-%d')
        schedule = load_schedule_from_db_sync(date_str)
        if schedule:
            current_schedules[date_str] = schedule
            print(f"  {date_str} ({day_name}): {schedule.get(day_name, 'нет данных')}")
    
    # Строим расписание на основе заявок
    print(f"\n{'='*60}")
    print("СТРОИМ РАСПИСАНИЕ НА ОСНОВЕ ЗАЯВОК")
    print(f"{'='*60}")
    
    if requests:
        schedule, removed_by_skipped = schedule_manager.build_schedule_from_requests(
            week_start, requests, employee_manager
        )
        
        print(f"\n📋 Построенное расписание после применения requests:")
        for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
            employees = schedule.get(day_name, [])
            print(f"  {day_name}: {len(employees)} сотрудников")
            if employees:
                # Проверяем на дубликаты
                employees_plain = [schedule_manager.get_plain_name_from_formatted(e) for e in employees]
                duplicates = [e for e in set(employees_plain) if employees_plain.count(e) > 1]
                if duplicates:
                    print(f"    ⚠️ ДУБЛИКАТЫ: {duplicates}")
                print(f"    {', '.join(employees[:5])}{'...' if len(employees) > 5 else ''}")
    else:
        print("⚠️ Нет заявок - используем default_schedule")
        schedule = {}
        for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
            schedule[day_name] = []
    
    # Форматируем default_schedule для сравнения
    print(f"\n{'='*60}")
    print("СРАВНЕНИЕ С DEFAULT_SCHEDULE")
    print(f"{'='*60}")
    
    # Конвертируем default_schedule из формата БД в список
    formatted_default = {}
    for day_name, places_dict in default_schedule_db.items():
        employees = []
        for place_key in sorted(places_dict.keys(), key=lambda x: (int(x.split('.')[0]), int(x.split('.')[1]))):
            name = places_dict.get(place_key, '')
            if name:
                formatted_name = employee_manager.format_employee_name(name)
                employees.append(formatted_name)
        formatted_default[day_name] = employees
    
    # Определяем измененные дни
    changed_days = set()
    final_schedule = {}
    
    for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
        schedule_employees = sorted([e.strip() for e in schedule.get(day_name, []) if e.strip()])
        default_employees = sorted([e.strip() for e in formatted_default.get(day_name, []) if e.strip()])
        
        print(f"\n📅 День {day_name}:")
        print(f"  schedule до дополнения: {len(schedule_employees)} сотрудников")
        if schedule_employees:
            # Проверяем на дубликаты
            employees_plain = [schedule_manager.get_plain_name_from_formatted(e) for e in schedule_employees]
            duplicates = [e for e in set(employees_plain) if employees_plain.count(e) > 1]
            if duplicates:
                print(f"    ⚠️ ДУБЛИКАТЫ: {duplicates}")
            print(f"    {', '.join(schedule_employees[:5])}{'...' if len(schedule_employees) > 5 else ''}")
        print(f"  default: {len(default_employees)} сотрудников")
        print(f"  отличаются до дополнения: {schedule_employees != default_employees}")
        
        if schedule_employees != default_employees:
            schedule_day = schedule.get(day_name, [])
            default_day = formatted_default.get(day_name, [])
            
            schedule_names = set([e.strip() for e in schedule_day if e.strip()])
            
            print(f"  До дополнения: {len(schedule_day)} сотрудников в schedule, {len(default_day)} в default")
            
            # Дополняем пустые места из default
            for emp in default_day:
                emp_stripped = emp.strip()
                emp_plain = schedule_manager.get_plain_name_from_formatted(emp_stripped)
                if emp_stripped and emp_stripped not in schedule_names:
                    if emp_plain not in removed_by_skipped.get(day_name, set()):
                        schedule_day.append(emp)
                        schedule_names.add(emp_stripped)
                        if len(schedule_day) >= len(default_day):
                            break
            
            final_employees = sorted([e.strip() for e in schedule_day if e.strip()])
            
            # Проверяем на дубликаты после дополнения
            employees_plain = [schedule_manager.get_plain_name_from_formatted(e) for e in final_employees]
            duplicates = [e for e in set(employees_plain) if employees_plain.count(e) > 1]
            if duplicates:
                print(f"    ⚠️ ДУБЛИКАТЫ ПОСЛЕ ДОПОЛНЕНИЯ: {duplicates}")
            
            print(f"  После дополнения: {len(final_employees)} сотрудников")
            print(f"    {', '.join(final_employees[:5])}{'...' if len(final_employees) > 5 else ''}")
            print(f"  После дополнения отличается от default: {final_employees != default_employees}")
            
            changed_days.add(day_name)
            final_schedule[day_name] = schedule_day
            print(f"  ✅ День {day_name} будет сохранен")
        else:
            print(f"  ❌ День {day_name} не изменился - не сохраняем")
    
    # Показываем результат для /full_schedule
    print(f"\n{'='*60}")
    print("РЕЗУЛЬТАТ ДЛЯ /full_schedule 2025-12-22")
    print(f"{'='*60}")
    
    for date, day_name in week_dates:
        date_str = date.strftime('%Y-%m-%d')
        if day_name in final_schedule:
            employees = final_schedule[day_name]
            # Проверяем на дубликаты
            employees_plain = [schedule_manager.get_plain_name_from_formatted(e) for e in employees]
            duplicates = [e for e in set(employees_plain) if employees_plain.count(e) > 1]
            status = "⚠️ ДУБЛИКАТЫ!" if duplicates else "✅"
            print(f"\n{status} {date_str} ({day_name}):")
            print(f"  {', '.join(employees)}")
            if duplicates:
                print(f"  ⚠️ ДУБЛИКАТЫ: {duplicates}")
        else:
            # Используется default_schedule
            employees = formatted_default.get(day_name, [])
            print(f"\n✅ {date_str} ({day_name}) - используется default_schedule:")
            print(f"  {', '.join(employees)}")

if __name__ == "__main__":
    main()

