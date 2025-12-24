#!/usr/bin/env python3
"""
Тестовый скрипт для воспроизведения команды /admin_rebuild_schedules_from_requests
с отслеживанием изменений в таблицах schedules, admins, requests, queue
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set
import pytz

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Пытаемся загрузить переменные окружения из .env файла
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Устанавливаем минимальные переменные окружения для тестирования
if not os.getenv('BOT_TOKEN'):
    os.environ['BOT_TOKEN'] = 'test_token'

# Проверяем наличие DATABASE_URL
if not os.getenv('DATABASE_URL') and not os.getenv('DATABASE_PUBLIC_URL'):
    if len(sys.argv) > 1:
        os.environ['DATABASE_URL'] = sys.argv[1]
        print(f"✅ Используется DATABASE_URL из аргументов командной строки")
    else:
        print("❌ Ошибка: DATABASE_URL не установлен")
        print("Использование: python3 test_rebuild_command.py <DATABASE_URL>")
        sys.exit(1)

from config import TIMEZONE, DATABASE_URL
from employee_manager import EmployeeManager
from schedule_manager import ScheduleManager
from database_sync import (
    load_default_schedule_from_db_sync,
    load_requests_from_db_sync,
    load_schedule_from_db_sync,
    load_admins_from_db_sync,
    load_queue_from_db_sync,
    _get_connection
)
from psycopg2.extras import RealDictCursor


def get_table_state(table_name: str) -> List[Dict]:
    """Получить состояние таблицы из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if table_name == 'admins':
                cur.execute("SELECT telegram_id FROM admins ORDER BY telegram_id")
            elif table_name == 'schedules':
                cur.execute("SELECT date, day_name, employees FROM schedules ORDER BY date, day_name")
            elif table_name == 'requests':
                cur.execute("SELECT week_start, employee_name, telegram_id, days_requested, days_skipped FROM requests ORDER BY week_start, telegram_id")
            elif table_name == 'queue':
                cur.execute("SELECT date, employee_name, telegram_id FROM queue ORDER BY date, telegram_id")
            else:
                return []
            
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Ошибка загрузки состояния таблицы {table_name}: {e}")
        return []
    finally:
        if conn:
            conn.close()


def print_table_state(table_name: str, state: List[Dict], label: str = ""):
    """Вывести состояние таблицы"""
    print(f"\n{'='*60}")
    print(f"{label} ТАБЛИЦА: {table_name.upper()}")
    print(f"{'='*60}")
    print(f"Количество записей: {len(state)}")
    
    if not state:
        print("  (таблица пуста)")
        return
    
    if table_name == 'admins':
        print(f"  Администраторы: {sorted([r['telegram_id'] for r in state])}")
    elif table_name == 'schedules':
        print(f"  Расписания:")
        for r in state[:10]:  # Показываем первые 10
            date_str = r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date'])
            employees = r['employees'][:100] if r['employees'] else ''
            print(f"    {date_str} ({r['day_name']}): {employees}...")
        if len(state) > 10:
            print(f"    ... и еще {len(state) - 10} записей")
    elif table_name == 'requests':
        print(f"  Заявки:")
        for r in state[:10]:
            week_str = r['week_start'].strftime('%Y-%m-%d') if isinstance(r['week_start'], datetime) else str(r['week_start'])
            print(f"    {week_str} - {r['employee_name']} (ID: {r['telegram_id']}): запрошены={r['days_requested']}, пропущены={r['days_skipped']}")
        if len(state) > 10:
            print(f"    ... и еще {len(state) - 10} записей")
    elif table_name == 'queue':
        print(f"  Очередь:")
        for r in state[:10]:
            date_str = r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date'])
            print(f"    {date_str} - {r['employee_name']} (ID: {r['telegram_id']})")
        if len(state) > 10:
            print(f"    ... и еще {len(state) - 10} записей")


def compare_table_states(before: List[Dict], after: List[Dict], table_name: str):
    """Сравнить состояния таблицы до и после"""
    print(f"\n{'='*60}")
    print(f"ИЗМЕНЕНИЯ В ТАБЛИЦЕ: {table_name.upper()}")
    print(f"{'='*60}")
    
    if table_name == 'admins':
        before_ids = set(r['telegram_id'] for r in before)
        after_ids = set(r['telegram_id'] for r in after)
        added = after_ids - before_ids
        removed = before_ids - after_ids
        if added:
            print(f"  ➕ Добавлено администраторов: {sorted(added)}")
        if removed:
            print(f"  🗑️ Удалено администраторов: {sorted(removed)}")
        if not added and not removed:
            print(f"  ✅ Изменений нет")
    
    elif table_name == 'schedules':
        before_dict = {}
        for r in before:
            date_str = r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date'])
            key = (date_str, r['day_name'])
            before_dict[key] = r
        
        after_dict = {}
        for r in after:
            date_str = r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date'])
            key = (date_str, r['day_name'])
            after_dict[key] = r
        
        added = set(after_dict.keys()) - set(before_dict.keys())
        removed = set(before_dict.keys()) - set(after_dict.keys())
        changed = []
        
        for key in set(before_dict.keys()) & set(after_dict.keys()):
            before_r = before_dict[key]
            after_r = after_dict[key]
            # Сравниваем содержимое, а не объекты
            if (before_r.get('employees') != after_r.get('employees') or
                before_r.get('day_name') != after_r.get('day_name')):
                changed.append(key)
        
        if added:
            print(f"  ➕ Добавлено расписаний: {len(added)}")
            for key in sorted(added)[:5]:
                date_str, day_name = key
                r = after_dict[key]
                employees = r['employees'][:100] if r['employees'] else ''
                print(f"    {date_str} ({day_name}): {employees}...")
        
        if removed:
            print(f"  🗑️ Удалено расписаний: {len(removed)}")
            for key in sorted(removed)[:5]:
                date_str, day_name = key
                r = before_dict[key]
                employees = r['employees'][:100] if r['employees'] else ''
                print(f"    {date_str} ({day_name}): {employees}...")
        
        if changed:
            print(f"  🔄 Изменено расписаний: {len(changed)}")
            for key in sorted(changed)[:5]:
                date_str, day_name = key
                before_r = before_dict[key]
                after_r = after_dict[key]
                print(f"    {date_str} ({day_name}):")
                print(f"      Было: {before_r['employees'][:100] if before_r['employees'] else ''}...")
                print(f"      Стало: {after_r['employees'][:100] if after_r['employees'] else ''}...")
        
        if not added and not removed and not changed:
            print(f"  ✅ Изменений нет")
    
    elif table_name == 'requests':
        before_dict = {}
        for r in before:
            key = (r['week_start'].strftime('%Y-%m-%d') if isinstance(r['week_start'], datetime) else str(r['week_start']), r['telegram_id'])
            before_dict[key] = r
        
        after_dict = {}
        for r in after:
            key = (r['week_start'].strftime('%Y-%m-%d') if isinstance(r['week_start'], datetime) else str(r['week_start']), r['telegram_id'])
            after_dict[key] = r
        
        added = set(after_dict.keys()) - set(before_dict.keys())
        removed = set(before_dict.keys()) - set(after_dict.keys())
        changed = []
        
        for key in set(before_dict.keys()) & set(after_dict.keys()):
            if before_dict[key] != after_dict[key]:
                changed.append(key)
        
        if added:
            print(f"  ➕ Добавлено заявок: {len(added)}")
        if removed:
            print(f"  🗑️ Удалено заявок: {len(removed)}")
        if changed:
            print(f"  🔄 Изменено заявок: {len(changed)}")
        if not added and not removed and not changed:
            print(f"  ✅ Изменений нет")
    
    elif table_name == 'queue':
        before_dict = {}
        for r in before:
            key = (r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date']), r['telegram_id'])
            before_dict[key] = r
        
        after_dict = {}
        for r in after:
            key = (r['date'].strftime('%Y-%m-%d') if isinstance(r['date'], datetime) else str(r['date']), r['telegram_id'])
            after_dict[key] = r
        
        added = set(after_dict.keys()) - set(before_dict.keys())
        removed = set(before_dict.keys()) - set(after_dict.keys())
        
        if added:
            print(f"  ➕ Добавлено в очередь: {len(added)}")
        if removed:
            print(f"  🗑️ Удалено из очереди: {len(removed)}")
        if not added and not removed:
            print(f"  ✅ Изменений нет")


def get_week_start(date: datetime) -> datetime:
    """Получить начало недели (понедельник) для даты"""
    days_since_monday = date.weekday()
    week_start = date - timedelta(days=days_since_monday)
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0)


def main():
    print("="*80)
    print("ТЕСТ КОМАНДЫ /admin_rebuild_schedules_from_requests")
    print("С ОТСЛЕЖИВАНИЕМ ИЗМЕНЕНИЙ В ТАБЛИЦАХ")
    print("="*80)
    
    # Загружаем состояние таблиц ДО выполнения команды
    print("\n" + "="*80)
    print("СОСТОЯНИЕ ТАБЛИЦ ДО ВЫПОЛНЕНИЯ КОМАНДЫ")
    print("="*80)
    
    tables_before = {}
    for table_name in ['admins', 'schedules', 'requests', 'queue']:
        state = get_table_state(table_name)
        tables_before[table_name] = state
        print_table_state(table_name, state, "ДО")
    
    # Инициализируем менеджеры
    print("\n" + "="*80)
    print("ИНИЦИАЛИЗАЦИЯ МЕНЕДЖЕРОВ")
    print("="*80)
    
    employee_manager = EmployeeManager()
    schedule_manager = ScheduleManager(employee_manager)
    
    employees_count = len(employee_manager.employees) if hasattr(employee_manager, 'employees') else 0
    print(f"✅ Загружено сотрудников: {employees_count}")
    
    # Определяем будущие недели (начиная со следующей недели)
    timezone = pytz.timezone(TIMEZONE)
    now = datetime.now(timezone)
    current_week_start = schedule_manager.get_week_start(now)
    
    # Находим все недели с заявками
    print("\n" + "="*80)
    print("ПОИСК НЕДЕЛЬ С ЗАЯВКАМИ")
    print("="*80)
    
    weeks_with_requests = set()
    conn = _get_connection()
    if conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT DISTINCT week_start FROM requests ORDER BY week_start")
                for row in cur.fetchall():
                    week_date = row['week_start']
                    if isinstance(week_date, datetime):
                        week_date = week_date.date()
                    if week_date > current_week_start.date():
                        weeks_with_requests.add(week_date)
        except Exception as e:
            logger.error(f"Ошибка поиска недель с заявками: {e}")
        finally:
            conn.close()
    
    print(f"Найдено недель с заявками (будущие): {len(weeks_with_requests)}")
    for week_date in sorted(weeks_with_requests)[:5]:
        print(f"  {week_date}")
    
    if not weeks_with_requests:
        print("⚠️ Нет будущих недель с заявками")
        return
    
    # Выполняем перестройку расписаний для каждой недели
    print("\n" + "="*80)
    print("ВЫПОЛНЕНИЕ ПЕРЕСТРОЙКИ РАСПИСАНИЙ")
    print("="*80)
    
    total_rebuilt = 0
    total_errors = 0
    
    for week_date in sorted(weeks_with_requests):
        week_start = datetime.combine(week_date, datetime.min.time()).replace(tzinfo=timezone)
        week_str = week_start.strftime('%Y-%m-%d')
        
        print(f"\n{'='*60}")
        print(f"ОБРАБОТКА НЕДЕЛИ: {week_str}")
        print(f"{'='*60}")
        
        # Загружаем заявки для недели
        requests = load_requests_from_db_sync(week_str)
        if not requests:
            print(f"  ⚠️ Нет заявок для недели {week_str}, пропускаем")
            continue
        
        print(f"  📋 Загружено {len(requests)} заявок:")
        for req in requests:
            print(f"    - {req['employee_name']}: запрошены={req['days_requested']}, пропущены={req['days_skipped']}")
        
        try:
            # Берем default_schedule как базу
            default_schedule = schedule_manager.load_default_schedule()
            default_schedule_list = schedule_manager._default_schedule_to_list(default_schedule)
            
            # Форматируем имена в default_schedule для сравнения
            formatted_default = {}
            for day, employees in default_schedule_list.items():
                formatted_default[day] = [employee_manager.format_employee_name(emp) for emp in employees]
            
            print(f"  📋 default_schedule содержит:")
            for day, emps in formatted_default.items():
                print(f"    {day}: {len(emps)} сотрудников")
            
            # Строим расписание на основе заявок
            schedule, removed_by_skipped = schedule_manager.build_schedule_from_requests(
                week_start, requests, employee_manager
            )
            
            print(f"  📋 Построенное расписание после применения requests:")
            for day, emps in schedule.items():
                print(f"    {day}: {len(emps)} сотрудников")
            
            # Определяем дни, которые реально отличаются от default после применения requests
            changed_days = set()
            final_schedule = {}
            
            for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
                schedule_employees = sorted([e.strip() for e in schedule.get(day_name, []) if e.strip()])
                default_employees = sorted([e.strip() for e in formatted_default.get(day_name, []) if e.strip()])
                
                if schedule_employees != default_employees:
                    # День изменился после применения requests - дополняем пустые места из default
                    schedule_day = schedule.get(day_name, [])
                    default_day = formatted_default.get(day_name, [])
                    
                    schedule_names = set([e.strip() for e in schedule_day if e.strip()])
                    
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
                    
                    changed_days.add(day_name)
                    final_schedule[day_name] = schedule_day
                    print(f"    ✅ День {day_name} будет сохранен (изменился)")
                else:
                    print(f"    ❌ День {day_name} не изменился - не сохраняем")
            
            print(f"  📋 Измененные дни: {changed_days}")
            
            # Сохраняем только измененные дни для будущих недель
            print(f"  💾 Сохранение расписания для недели {week_str}...")
            schedule_manager.save_schedule_for_week(
                week_start, 
                final_schedule, 
                only_changed_days=True, 
                employee_manager=employee_manager, 
                changed_days=changed_days
            )
            
            total_rebuilt += 1
            print(f"  ✅ Расписание перестроено")
            
        except Exception as e:
            logger.error(f"Ошибка перестройки расписания для недели {week_str}: {e}", exc_info=True)
            total_errors += 1
            print(f"  ❌ Ошибка: {e}")
    
    print(f"\n{'='*80}")
    print(f"ИТОГО: Перестроено {total_rebuilt} расписаний, ошибок {total_errors}")
    print(f"{'='*80}")
    
    # Загружаем состояние таблиц ПОСЛЕ выполнения команды
    print("\n" + "="*80)
    print("СОСТОЯНИЕ ТАБЛИЦ ПОСЛЕ ВЫПОЛНЕНИЯ КОМАНДЫ")
    print("="*80)
    
    tables_after = {}
    for table_name in ['admins', 'schedules', 'requests', 'queue']:
        state = get_table_state(table_name)
        tables_after[table_name] = state
        print_table_state(table_name, state, "ПОСЛЕ")
    
    # Сравниваем состояния
    print("\n" + "="*80)
    print("СРАВНЕНИЕ СОСТОЯНИЙ ТАБЛИЦ")
    print("="*80)
    
    for table_name in ['admins', 'schedules', 'requests', 'queue']:
        compare_table_states(tables_before[table_name], tables_after[table_name], table_name)
    
    print("\n" + "="*80)
    print("ТЕСТ ЗАВЕРШЕН")
    print("="*80)


if __name__ == "__main__":
    main()

