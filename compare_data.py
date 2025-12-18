"""
Скрипт для сравнения данных между Google Sheets и PostgreSQL
"""
import asyncio
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Устанавливаем переменные окружения
os.environ['BOT_TOKEN'] = os.getenv('BOT_TOKEN', 'comparison_token')
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from config import (
    USE_GOOGLE_SHEETS, SHEET_EMPLOYEES, SHEET_ADMINS, SHEET_PENDING_EMPLOYEES,
    SHEET_SCHEDULES, SHEET_DEFAULT_SCHEDULE, SHEET_REQUESTS, SHEET_QUEUE
)
from database import (
    init_db, test_connection, close_db,
    load_admins_from_db, load_employees_from_db, load_pending_employees_from_db,
    load_default_schedule_from_db, load_schedule_from_db, load_requests_from_db,
    load_queue_from_db
)
from utils import get_header_start_idx, filter_empty_rows

# Импортируем Google Sheets Manager
if USE_GOOGLE_SHEETS:
    try:
        from google_sheets_manager import GoogleSheetsManager
    except ImportError:
        print("❌ Не удалось импортировать GoogleSheetsManager")
        sys.exit(1)
else:
    print("❌ USE_GOOGLE_SHEETS отключен")
    sys.exit(1)


def normalize_employee_data(manual_name: str, telegram_name: str, username: str, approved: bool) -> str:
    """Нормализовать данные сотрудника для сравнения"""
    return f"{manual_name}:{telegram_name or ''}:{username or ''}:{approved}"


async def compare_admins(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить администраторов"""
    print("\n📋 Сравнение администраторов...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_ADMINS)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['admin_id', 'telegram_id', 'ID'])
    
    sheets_admins = set()
    for row in rows[start_idx:]:
        if row and row[0]:
            try:
                admin_id = int(row[0].strip())
                sheets_admins.add(admin_id)
            except ValueError:
                continue
    
    # Загружаем из PostgreSQL
    db_admins = await load_admins_from_db()
    
    # Сравнение
    only_in_sheets = sheets_admins - db_admins
    only_in_db = db_admins - sheets_admins
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0
    
    result = {
        'sheets_count': len(sheets_admins),
        'db_count': len(db_admins),
        'match': match,
        'only_in_sheets': sorted(only_in_sheets),
        'only_in_db': sorted(only_in_db)
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_admins)} записей")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_admins)}, PostgreSQL: {len(db_admins)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {only_in_sheets}")
        if only_in_db:
            print(f"   Только в PostgreSQL: {only_in_db}")
    
    return match, result


async def compare_employees(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить сотрудников"""
    print("\n👥 Сравнение сотрудников...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_EMPLOYEES)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['manual_name', 'telegram_name', 'telegram_id', 'username'])
    
    sheets_employees = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            parts = row[0].split(':') if len(row) == 1 else [row[i] if i < len(row) else '' for i in range(4)]
            
            if len(parts) >= 3:
                manual_name = parts[0].strip()
                telegram_name = parts[1].strip() if len(parts) > 1 else ''
                telegram_id = int(parts[2].strip()) if len(parts) > 2 and parts[2].strip() else None
                username = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                
                if telegram_id and manual_name:
                    sheets_employees[telegram_id] = normalize_employee_data(
                        manual_name, telegram_name, username, True
                    )
        except (ValueError, IndexError):
            continue
    
    # Загружаем из PostgreSQL
    db_employees = await load_employees_from_db()
    db_employees_normalized = {
        tid: normalize_employee_data(manual_name, telegram_name, username, approved)
        for tid, (manual_name, telegram_name, username, approved) in db_employees.items()
    }
    
    # Сравнение
    only_in_sheets = set(sheets_employees.keys()) - set(db_employees_normalized.keys())
    only_in_db = set(db_employees_normalized.keys()) - set(sheets_employees.keys())
    different_content = []
    
    for tid in set(sheets_employees.keys()) & set(db_employees_normalized.keys()):
        if sheets_employees[tid] != db_employees_normalized[tid]:
            different_content.append({
                'telegram_id': tid,
                'sheets': sheets_employees[tid],
                'db': db_employees_normalized[tid]
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_employees),
        'db_count': len(db_employees_normalized),
        'match': match,
        'only_in_sheets': sorted(only_in_sheets),
        'only_in_db': sorted(only_in_db),
        'different_content': different_content[:5]  # Показываем первые 5
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_employees)} записей")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_employees)}, PostgreSQL: {len(db_employees_normalized)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {only_in_sheets}")
        if only_in_db:
            print(f"   Только в PostgreSQL: {only_in_db}")
        if different_content:
            print(f"   Различается содержимое у {len(different_content)} записей:")
            for diff in different_content[:3]:
                print(f"     ID {diff['telegram_id']}:")
                print(f"       Google Sheets: {diff['sheets']}")
                print(f"       PostgreSQL: {diff['db']}")
    
    return match, result


async def compare_pending_employees(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить отложенных сотрудников"""
    print("\n⏳ Сравнение отложенных сотрудников...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_PENDING_EMPLOYEES)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['username', 'manual_name'])
    
    sheets_pending = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 2:
            continue
        
        try:
            username = row[0].strip() if row[0] else None
            manual_name = row[1].strip() if len(row) > 1 and row[1] else None
            
            if username and manual_name:
                sheets_pending[username] = manual_name
        except Exception:
            continue
    
    # Загружаем из PostgreSQL
    db_pending = await load_pending_employees_from_db()
    
    # Сравнение
    only_in_sheets = set(sheets_pending.keys()) - set(db_pending.keys())
    only_in_db = set(db_pending.keys()) - set(sheets_pending.keys())
    different_content = []
    
    for username in set(sheets_pending.keys()) & set(db_pending.keys()):
        if sheets_pending[username] != db_pending[username]:
            different_content.append({
                'username': username,
                'sheets': sheets_pending[username],
                'db': db_pending[username]
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_pending),
        'db_count': len(db_pending),
        'match': match,
        'only_in_sheets': list(only_in_sheets),
        'only_in_db': list(only_in_db),
        'different_content': different_content
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_pending)} записей")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_pending)}, PostgreSQL: {len(db_pending)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {only_in_sheets}")
        if only_in_db:
            print(f"   Только в PostgreSQL: {only_in_db}")
        if different_content:
            print(f"   Различается содержимое: {different_content}")
    
    return match, result


async def compare_default_schedule(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить расписание по умолчанию"""
    print("\n📅 Сравнение расписания по умолчанию...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['day_name', 'places_json'])
    
    sheets_schedule = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 2:
            continue
        
        try:
            day_name = row[0].strip() if row[0] else None
            places_json_str = row[1].strip() if len(row) > 1 and row[1] else None
            
            if day_name and places_json_str:
                try:
                    places_dict = json.loads(places_json_str)
                    sheets_schedule[day_name] = places_dict
                except json.JSONDecodeError:
                    continue
        except Exception:
            continue
    
    # Загружаем из PostgreSQL
    db_schedule = await load_default_schedule_from_db()
    
    # Сравнение
    only_in_sheets = set(sheets_schedule.keys()) - set(db_schedule.keys())
    only_in_db = set(db_schedule.keys()) - set(sheets_schedule.keys())
    different_content = []
    
    for day_name in set(sheets_schedule.keys()) & set(db_schedule.keys()):
        if sheets_schedule[day_name] != db_schedule[day_name]:
            different_content.append({
                'day_name': day_name,
                'sheets_places': len(sheets_schedule[day_name]),
                'db_places': len(db_schedule[day_name])
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_schedule),
        'db_count': len(db_schedule),
        'match': match,
        'only_in_sheets': list(only_in_sheets),
        'only_in_db': list(only_in_db),
        'different_content': different_content
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_schedule)} дней")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_schedule)} дней, PostgreSQL: {len(db_schedule)} дней")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {only_in_sheets}")
        if only_in_db:
            print(f"   Только в PostgreSQL: {only_in_db}")
        if different_content:
            print(f"   Различается содержимое у {len(different_content)} дней: {different_content}")
    
    return match, result


async def compare_schedules(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить расписания на даты"""
    print("\n📆 Сравнение расписаний на даты...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'day_name', 'employees'])
    
    sheets_schedules = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            date_str = row[0].strip() if row[0] else None
            day_name = row[1].strip() if len(row) > 1 and row[1] else None
            employees_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if date_str and day_name and employees_str:
                try:
                    datetime.strptime(date_str, '%Y-%m-%d')
                    sheets_schedules[date_str] = {
                        'day_name': day_name,
                        'employees': employees_str
                    }
                except ValueError:
                    continue
        except Exception:
            continue
    
    # Загружаем из PostgreSQL (проверяем все даты из Google Sheets)
    db_schedules = {}
    for date_str in sheets_schedules.keys():
        schedule = await load_schedule_from_db(date_str)
        if schedule:
            for day_name, employees_str in schedule.items():
                db_schedules[date_str] = {
                    'day_name': day_name,
                    'employees': employees_str
                }
    
    # Сравнение
    only_in_sheets = set(sheets_schedules.keys()) - set(db_schedules.keys())
    only_in_db = set(db_schedules.keys()) - set(sheets_schedules.keys())
    different_content = []
    
    for date_str in set(sheets_schedules.keys()) & set(db_schedules.keys()):
        sheets_data = sheets_schedules[date_str]
        db_data = db_schedules[date_str]
        
        # Нормализуем employees (сортируем имена для сравнения)
        sheets_employees = sorted([e.strip() for e in sheets_data['employees'].split(',') if e.strip()])
        db_employees = sorted([e.strip() for e in db_data['employees'].split(',') if e.strip()])
        
        if sheets_data['day_name'] != db_data['day_name'] or sheets_employees != db_employees:
            different_content.append({
                'date': date_str,
                'sheets': f"{sheets_data['day_name']}: {len(sheets_employees)} сотрудников",
                'db': f"{db_data['day_name']}: {len(db_employees)} сотрудников"
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_schedules),
        'db_count': len(db_schedules),
        'match': match,
        'only_in_sheets': sorted(only_in_sheets),
        'only_in_db': sorted(only_in_db),
        'different_content': different_content[:5]
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_schedules)} расписаний")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_schedules)}, PostgreSQL: {len(db_schedules)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {len(only_in_sheets)} дат (первые 5: {only_in_sheets[:5]})")
        if only_in_db:
            print(f"   Только в PostgreSQL: {len(only_in_db)} дат (первые 5: {only_in_db[:5]})")
        if different_content:
            print(f"   Различается содержимое у {len(different_content)} расписаний:")
            for diff in different_content[:3]:
                date_str = diff['date']
                # Загружаем детальные данные для этой даты
                sheets_row = None
                for row in rows[start_idx:]:
                    if row[0] and row[0].strip() == date_str:
                        sheets_row = row
                        break
                
                db_schedule = await load_schedule_from_db(date_str)
                
                print(f"     {date_str}:")
                if sheets_row:
                    sheets_employees = [e.strip() for e in sheets_row[2].split(',') if e.strip()] if len(sheets_row) > 2 and sheets_row[2] else []
                    print(f"       Google Sheets: {sheets_row[1] if len(sheets_row) > 1 else 'N/A'}: {len(sheets_employees)} сотрудников")
                    print(f"         Сотрудники: {', '.join(sheets_employees[:10])}{'...' if len(sheets_employees) > 10 else ''}")
                if db_schedule:
                    for day_name, employees_str in db_schedule.items():
                        db_employees = [e.strip() for e in employees_str.split(',') if e.strip()] if employees_str else []
                        print(f"       PostgreSQL: {day_name}: {len(db_employees)} сотрудников")
                        print(f"         Сотрудники: {', '.join(db_employees[:10])}{'...' if len(db_employees) > 10 else ''}")
    
    return match, result


async def compare_requests(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить заявки"""
    print("\n📝 Сравнение заявок...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_REQUESTS)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped'])
    
    sheets_requests = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            week_start_str = row[0].strip() if row[0] else None
            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
            telegram_id_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if week_start_str and employee_name and telegram_id_str:
                try:
                    datetime.strptime(week_start_str, '%Y-%m-%d')
                    telegram_id = int(telegram_id_str)
                    key = (week_start_str, telegram_id)
                    
                    days_requested_str = row[3].strip() if len(row) > 3 and row[3] else None
                    days_skipped_str = row[4].strip() if len(row) > 4 and row[4] else None
                    
                    days_requested = sorted([d.strip() for d in days_requested_str.split(',')]) if days_requested_str else []
                    days_skipped = sorted([d.strip() for d in days_skipped_str.split(',')]) if days_skipped_str else []
                    
                    sheets_requests[key] = {
                        'employee_name': employee_name,
                        'days_requested': days_requested,
                        'days_skipped': days_skipped
                    }
                except (ValueError, TypeError):
                    continue
        except Exception:
            continue
    
    # Загружаем из PostgreSQL
    db_requests = {}
    for week_start_str in set([key[0] for key in sheets_requests.keys()]):
        requests = await load_requests_from_db(week_start_str)
        for req in requests:
            key = (week_start_str, req['telegram_id'])
            db_requests[key] = {
                'employee_name': req['employee_name'],
                'days_requested': sorted(req['days_requested']),
                'days_skipped': sorted(req['days_skipped'])
            }
    
    # Сравнение
    only_in_sheets = set(sheets_requests.keys()) - set(db_requests.keys())
    only_in_db = set(db_requests.keys()) - set(sheets_requests.keys())
    different_content = []
    
    for key in set(sheets_requests.keys()) & set(db_requests.keys()):
        if sheets_requests[key] != db_requests[key]:
            different_content.append({
                'key': key,
                'sheets': sheets_requests[key],
                'db': db_requests[key]
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_requests),
        'db_count': len(db_requests),
        'match': match,
        'only_in_sheets': list(only_in_sheets),
        'only_in_db': list(only_in_db),
        'different_content': different_content[:5]
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_requests)} заявок")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_requests)}, PostgreSQL: {len(db_requests)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {len(only_in_sheets)} заявок")
        if only_in_db:
            print(f"   Только в PostgreSQL: {len(only_in_db)} заявок")
        if different_content:
            print(f"   Различается содержимое у {len(different_content)} заявок:")
            for diff in different_content[:3]:
                week_start, telegram_id = diff['key']
                sheets_data = diff['sheets']
                db_data = diff['db']
                print(f"     Неделя {week_start}, Сотрудник: {sheets_data['employee_name']} (ID: {telegram_id})")
                print(f"       Google Sheets:")
                print(f"         Запрошено: {', '.join(sheets_data['days_requested']) if sheets_data['days_requested'] else '(пусто)'}")
                print(f"         Пропущено: {', '.join(sheets_data['days_skipped']) if sheets_data['days_skipped'] else '(пусто)'}")
                print(f"       PostgreSQL:")
                print(f"         Запрошено: {', '.join(db_data['days_requested']) if db_data['days_requested'] else '(пусто)'}")
                print(f"         Пропущено: {', '.join(db_data['days_skipped']) if db_data['days_skipped'] else '(пусто)'}")
    
    return match, result


async def compare_queue(sheets_manager: GoogleSheetsManager) -> Tuple[bool, Dict]:
    """Сравнить очередь"""
    print("\n⏳ Сравнение очереди...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_QUEUE)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата', 'employee_name'])
    
    sheets_queue = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            date_str = row[0].strip() if row[0] else None
            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
            telegram_id_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if date_str and employee_name and telegram_id_str:
                try:
                    datetime.strptime(date_str, '%Y-%m-%d')
                    telegram_id = int(telegram_id_str)
                    key = (date_str, telegram_id)
                    sheets_queue[key] = {
                        'employee_name': employee_name,
                        'telegram_id': telegram_id
                    }
                except (ValueError, TypeError):
                    continue
        except Exception:
            continue
    
    # Загружаем из PostgreSQL (проверяем все даты из Google Sheets)
    db_queue = {}
    for date_str in set([key[0] for key in sheets_queue.keys()]):
        queue = await load_queue_from_db(date_str)
        for entry in queue:
            key = (date_str, entry['telegram_id'])
            db_queue[key] = entry
    
    # Сравнение
    only_in_sheets = set(sheets_queue.keys()) - set(db_queue.keys())
    only_in_db = set(db_queue.keys()) - set(sheets_queue.keys())
    different_content = []
    
    for key in set(sheets_queue.keys()) & set(db_queue.keys()):
        if sheets_queue[key] != db_queue[key]:
            different_content.append({
                'key': key,
                'sheets': sheets_queue[key],
                'db': db_queue[key]
            })
    
    match = len(only_in_sheets) == 0 and len(only_in_db) == 0 and len(different_content) == 0
    
    result = {
        'sheets_count': len(sheets_queue),
        'db_count': len(db_queue),
        'match': match,
        'only_in_sheets': list(only_in_sheets),
        'only_in_db': list(only_in_db),
        'different_content': different_content[:5]
    }
    
    if match:
        print(f"   ✅ Совпадают: {len(sheets_queue)} записей")
    else:
        print(f"   ❌ Не совпадают!")
        print(f"   Google Sheets: {len(sheets_queue)}, PostgreSQL: {len(db_queue)}")
        if only_in_sheets:
            print(f"   Только в Google Sheets: {len(only_in_sheets)} записей")
            for key in list(only_in_sheets)[:3]:
                date_str, telegram_id = key
                entry = sheets_queue[key]
                print(f"     {date_str}: {entry['employee_name']} (ID: {telegram_id})")
        if only_in_db:
            print(f"   Только в PostgreSQL: {len(only_in_db)} записей")
            for key in list(only_in_db)[:3]:
                date_str, telegram_id = key
                entry = db_queue[key]
                print(f"     {date_str}: {entry['employee_name']} (ID: {telegram_id})")
        if different_content:
            print(f"   Различается содержимое у {len(different_content)} записей")
    
    return match, result


async def main():
    """Основная функция сравнения"""
    print("="*60)
    print("🔍 Сравнение данных Google Sheets и PostgreSQL")
    print("="*60)
    
    # Инициализация PostgreSQL
    print("\n1️⃣ Инициализация PostgreSQL...")
    success = await init_db()
    if not success:
        print("❌ Не удалось инициализировать PostgreSQL")
        return
    
    if not await test_connection():
        print("❌ Ошибка подключения к PostgreSQL")
        await close_db()
        return
    
    # Инициализация Google Sheets
    print("\n2️⃣ Инициализация Google Sheets...")
    try:
        sheets_manager = GoogleSheetsManager()
        if not sheets_manager.is_available():
            print("❌ Google Sheets недоступен")
            await close_db()
            return
    except Exception as e:
        print(f"❌ Ошибка подключения к Google Sheets: {e}")
        await close_db()
        return
    
    # Сравнение данных
    print("\n3️⃣ Сравнение данных...")
    print("-"*60)
    
    results = {}
    all_match = True
    
    match, result = await compare_admins(sheets_manager)
    results['admins'] = result
    all_match = all_match and match
    
    match, result = await compare_employees(sheets_manager)
    results['employees'] = result
    all_match = all_match and match
    
    match, result = await compare_pending_employees(sheets_manager)
    results['pending_employees'] = result
    all_match = all_match and match
    
    match, result = await compare_default_schedule(sheets_manager)
    results['default_schedule'] = result
    all_match = all_match and match
    
    match, result = await compare_schedules(sheets_manager)
    results['schedules'] = result
    all_match = all_match and match
    
    match, result = await compare_requests(sheets_manager)
    results['requests'] = result
    all_match = all_match and match
    
    match, result = await compare_queue(sheets_manager)
    results['queue'] = result
    all_match = all_match and match
    
    # Итоги
    print("\n" + "="*60)
    print("📊 Итоги сравнения:")
    print("="*60)
    
    for key, result in results.items():
        status = "✅" if result['match'] else "❌"
        print(f"  {status} {key}:")
        print(f"    Google Sheets: {result['sheets_count']} записей")
        print(f"    PostgreSQL: {result['db_count']} записей")
        if not result['match']:
            if result.get('only_in_sheets'):
                print(f"    ⚠️ Только в Google Sheets: {len(result['only_in_sheets'])}")
            if result.get('only_in_db'):
                print(f"    ⚠️ Только в PostgreSQL: {len(result['only_in_db'])}")
            if result.get('different_content'):
                print(f"    ⚠️ Различается содержимое: {len(result['different_content'])}")
    
    if all_match:
        print("\n✅ Все данные полностью совпадают!")
    else:
        print("\n⚠️ Обнаружены различия в данных")
    
    # Закрытие подключения
    print("\n4️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Сравнение завершено!")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(main())

