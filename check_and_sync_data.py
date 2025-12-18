"""
Скрипт для проверки и синхронизации данных между Google Sheets и PostgreSQL
Использует синхронные функции для работы с PostgreSQL
"""
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Set

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Устанавливаем переменные окружения
os.environ['BOT_TOKEN'] = os.getenv('BOT_TOKEN', 'check_token')
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from config import (
    USE_GOOGLE_SHEETS, SHEET_EMPLOYEES, SHEET_ADMINS, SHEET_PENDING_EMPLOYEES,
    SHEET_SCHEDULES, SHEET_DEFAULT_SCHEDULE, SHEET_REQUESTS, SHEET_QUEUE
)
from database_sync import (
    load_admins_from_db_sync, load_employees_from_db_sync, load_pending_employees_from_db_sync,
    load_default_schedule_from_db_sync, load_schedule_from_db_sync, load_requests_from_db_sync,
    load_queue_from_db_sync,
    save_admins_to_db_sync, save_employee_to_db_sync, save_pending_employee_to_db_sync,
    save_default_schedule_to_db_sync, save_schedule_to_db_sync, save_request_to_db_sync,
    add_to_queue_db_sync, remove_pending_employee_from_db_sync
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


def compare_and_sync_admins(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать администраторов"""
    print("\n👑 Проверка администраторов...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_ADMINS)
    if not rows:
        print("⚠️ Google Sheets: администраторы не найдены")
        sheets_admins = set()
    else:
        sheets_admins = set()
        for row in rows:
            if row and row[0].strip():
                try:
                    admin_id = int(row[0].strip())
                    sheets_admins.add(admin_id)
                except ValueError:
                    continue
    
    # Загружаем из PostgreSQL
    db_admins = load_admins_from_db_sync()
    
    print(f"   Google Sheets: {len(sheets_admins)} администраторов")
    print(f"   PostgreSQL: {len(db_admins)} администраторов")
    
    if sheets_admins != db_admins:
        print(f"   ⚠️ Различия найдены!")
        print(f"   Только в Google Sheets: {sheets_admins - db_admins}")
        print(f"   Только в PostgreSQL: {db_admins - sheets_admins}")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        save_admins_to_db_sync(sheets_admins)
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_employees(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать сотрудников"""
    print("\n👥 Проверка сотрудников...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_EMPLOYEES)
    if not rows:
        print("⚠️ Google Sheets: сотрудники не найдены")
        sheets_employees = {}
    else:
        from utils import filter_empty_rows
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['manual_name', 'Имя вручную'])
        sheets_employees = {}
        for row in rows[start_idx:]:
            if len(row) < 3 or not row[0] or not row[2]:
                continue
            try:
                manual_name = row[0].strip()
                telegram_name = row[1].strip() if len(row) > 1 and row[1].strip() else manual_name
                telegram_id = int(row[2].strip())
                username = row[3].strip() if len(row) > 3 and row[3].strip() else None
                # Если загружаем из Google Sheets, считаем что был добавлен админом
                approved = True
                sheets_employees[telegram_id] = (manual_name, telegram_name, username, approved)
            except (ValueError, IndexError):
                continue
    
    # Загружаем из PostgreSQL
    db_employees = load_employees_from_db_sync()
    
    print(f"   Google Sheets: {len(sheets_employees)} сотрудников")
    print(f"   PostgreSQL: {len(db_employees)} сотрудников")
    
    differences = False
    # Проверяем различия
    all_ids = set(sheets_employees.keys()) | set(db_employees.keys())
    for telegram_id in all_ids:
        sheets_data = sheets_employees.get(telegram_id)
        db_data = db_employees.get(telegram_id)
        if sheets_data != db_data:
            differences = True
            break
    
    if differences or len(sheets_employees) != len(db_employees):
        print(f"   ⚠️ Различия найдены!")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        for telegram_id, (manual_name, telegram_name, username, approved) in sheets_employees.items():
            save_employee_to_db_sync(telegram_id, manual_name, telegram_name, username, approved)
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_pending_employees(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать отложенных сотрудников"""
    print("\n⏳ Проверка отложенных сотрудников...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_PENDING_EMPLOYEES)
    if not rows:
        print("⚠️ Google Sheets: отложенные сотрудники не найдены")
        sheets_pending = {}
    else:
        header_idx = get_header_start_idx(rows, ['username', 'manual_name'])
        if isinstance(header_idx, tuple):
            header_idx = header_idx[0]
        sheets_pending = {}
        for row in rows[header_idx + 1:]:
            if not row or len(row) < 2:
                continue
            username = row[0].strip().lower().lstrip('@')
            manual_name = row[1].strip() if len(row) > 1 else ''
            if username:
                sheets_pending[username] = manual_name
    
    # Загружаем из PostgreSQL
    db_pending = load_pending_employees_from_db_sync()
    
    print(f"   Google Sheets: {len(sheets_pending)} отложенных сотрудников")
    print(f"   PostgreSQL: {len(db_pending)} отложенных сотрудников")
    
    if sheets_pending != db_pending:
        print(f"   ⚠️ Различия найдены!")
        print(f"   Только в Google Sheets: {set(sheets_pending.keys()) - set(db_pending.keys())}")
        print(f"   Только в PostgreSQL: {set(db_pending.keys()) - set(sheets_pending.keys())}")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        # Удаляем тех, кого нет в Google Sheets
        for username in db_pending:
            if username not in sheets_pending:
                remove_pending_employee_from_db_sync(username)
        # Добавляем/обновляем тех, кто есть в Google Sheets
        for username, manual_name in sheets_pending.items():
            save_pending_employee_to_db_sync(username, manual_name)
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_default_schedule(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать расписание по умолчанию"""
    print("\n📋 Проверка расписания по умолчанию...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
    if not rows:
        print("⚠️ Google Sheets: расписание по умолчанию не найдено")
        sheets_schedule = {}
    else:
        start_idx, _ = get_header_start_idx(rows, ['day_name', 'places_json'])
        sheets_schedule = {}
        for row in rows[start_idx:]:
            if not row or len(row) < 2:
                continue
            day_name = row[0].strip()
            places_json = row[1].strip() if len(row) > 1 else '{}'
            try:
                places_dict = json.loads(places_json)
                sheets_schedule[day_name] = places_dict
            except json.JSONDecodeError:
                continue
    
    # Загружаем из PostgreSQL
    db_schedule = load_default_schedule_from_db_sync()
    
    print(f"   Google Sheets: {len(sheets_schedule)} дней")
    print(f"   PostgreSQL: {len(db_schedule)} дней")
    
    if sheets_schedule != db_schedule:
        print(f"   ⚠️ Различия найдены!")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        save_default_schedule_to_db_sync(sheets_schedule)
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_schedules(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать расписания"""
    print("\n📅 Проверка расписаний...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
    if not rows:
        print("⚠️ Google Sheets: расписания не найдены")
        sheets_schedules = {}
    else:
        header_idx = get_header_start_idx(rows, ['date', 'day_name', 'employees'])
        if isinstance(header_idx, tuple):
            header_idx = header_idx[0]
        sheets_schedules = {}
        for row in rows[header_idx + 1:]:
            if not row or len(row) < 3:
                continue
            date_str = row[0].strip()
            day_name = row[1].strip() if len(row) > 1 else ''
            employees = row[2].strip() if len(row) > 2 else ''
            if date_str:
                sheets_schedules[date_str] = {day_name: employees}
    
    # Загружаем из PostgreSQL (проверяем все даты из Google Sheets)
    differences = False
    synced_count = 0
    
    for date_str in sheets_schedules:
        db_schedule = load_schedule_from_db_sync(date_str)
        sheets_data = sheets_schedules[date_str]
        
        if db_schedule != sheets_data:
            differences = True
            print(f"   ⚠️ Различия для {date_str}:")
            print(f"      Google Sheets: {sheets_data}")
            print(f"      PostgreSQL: {db_schedule}")
            # Синхронизируем
            for day_name, employees in sheets_data.items():
                save_schedule_to_db_sync(date_str, day_name, employees)
            synced_count += 1
    
    print(f"   Google Sheets: {len(sheets_schedules)} расписаний")
    print(f"   PostgreSQL: проверено {len(sheets_schedules)} расписаний")
    
    if differences:
        print(f"   🔄 Синхронизировано {synced_count} расписаний")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_requests(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать заявки"""
    print("\n📝 Проверка заявок...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_REQUESTS)
    if not rows:
        print("⚠️ Google Sheets: заявки не найдены")
        sheets_requests = {}
    else:
        header_idx = get_header_start_idx(rows, ['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped'])
        if isinstance(header_idx, tuple):
            header_idx = header_idx[0]
        sheets_requests = {}
        for row in rows[header_idx + 1:]:
            if not row or len(row) < 5:
                continue
            week_start = row[0].strip()
            employee_name = row[1].strip() if len(row) > 1 else ''
            try:
                telegram_id = int(row[2].strip()) if len(row) > 2 and row[2].strip() else 0
            except ValueError:
                continue
            days_requested = row[3].strip().split(',') if len(row) > 3 and row[3].strip() else []
            days_skipped = row[4].strip().split(',') if len(row) > 4 and row[4].strip() else []
            if week_start:
                key = (week_start, telegram_id)
                sheets_requests[key] = {
                    'employee_name': employee_name,
                    'telegram_id': telegram_id,
                    'days_requested': [d.strip() for d in days_requested if d.strip()],
                    'days_skipped': [d.strip() for d in days_skipped if d.strip()]
                }
    
    # Загружаем из PostgreSQL (по неделям из Google Sheets)
    differences = False
    synced_count = 0
    
    for (week_start, telegram_id), sheets_data in sheets_requests.items():
        db_requests = load_requests_from_db_sync(week_start)
        db_data = None
        for req in db_requests:
            if req['telegram_id'] == telegram_id:
                db_data = req
                break
        
        if db_data != sheets_data:
            differences = True
            print(f"   ⚠️ Различия для недели {week_start}, сотрудник {telegram_id}")
            # Синхронизируем
            save_request_to_db_sync(
                week_start,
                sheets_data['employee_name'],
                sheets_data['telegram_id'],
                sheets_data['days_requested'],
                sheets_data['days_skipped']
            )
            synced_count += 1
    
    print(f"   Google Sheets: {len(sheets_requests)} заявок")
    print(f"   PostgreSQL: проверено {len(sheets_requests)} заявок")
    
    if differences:
        print(f"   🔄 Синхронизировано {synced_count} заявок")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_queue(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать очередь"""
    print("\n⏰ Проверка очереди...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_QUEUE)
    if not rows:
        print("⚠️ Google Sheets: очередь не найдена")
        sheets_queue = {}
    else:
        header_idx = get_header_start_idx(rows, ['date', 'employee_name', 'telegram_id'])
        if isinstance(header_idx, tuple):
            header_idx = header_idx[0]
        sheets_queue = {}
        for row in rows[header_idx + 1:]:
            if not row or len(row) < 3:
                continue
            date_str = row[0].strip()
            employee_name = row[1].strip() if len(row) > 1 else ''
            try:
                telegram_id = int(row[2].strip()) if len(row) > 2 and row[2].strip() else 0
            except ValueError:
                continue
            if date_str:
                if date_str not in sheets_queue:
                    sheets_queue[date_str] = []
                sheets_queue[date_str].append({
                    'employee_name': employee_name,
                    'telegram_id': telegram_id
                })
    
    # Загружаем из PostgreSQL (проверяем все даты из Google Sheets)
    differences = False
    synced_count = 0
    
    for date_str in sheets_queue:
        db_queue = load_queue_from_db_sync(date_str)
        sheets_data = sheets_queue[date_str]
        
        # Сравниваем
        db_dict = {(q['employee_name'], q['telegram_id']) for q in db_queue}
        sheets_dict = {(q['employee_name'], q['telegram_id']) for q in sheets_data}
        
        if db_dict != sheets_dict:
            differences = True
            print(f"   ⚠️ Различия для {date_str}:")
            print(f"      Google Sheets: {len(sheets_data)} записей")
            print(f"      PostgreSQL: {len(db_queue)} записей")
            # Синхронизируем (удаляем все и добавляем заново из Google Sheets)
            # Удаляем все записи для этой даты
            for q in db_queue:
                from database_sync import remove_from_queue_db_sync
                remove_from_queue_db_sync(date_str, q['telegram_id'])
            # Добавляем из Google Sheets
            for q in sheets_data:
                add_to_queue_db_sync(date_str, q['employee_name'], q['telegram_id'])
            synced_count += 1
    
    print(f"   Google Sheets: {len(sheets_queue)} дат в очереди")
    print(f"   PostgreSQL: проверено {len(sheets_queue)} дат")
    
    if differences:
        print(f"   🔄 Синхронизировано {synced_count} дат")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def main():
    """Основная функция"""
    print("=" * 60)
    print("🔍 Проверка и синхронизация данных")
    print("   Google Sheets → PostgreSQL")
    print("=" * 60)
    
    # Инициализируем Google Sheets Manager
    sheets_manager = GoogleSheetsManager()
    if not sheets_manager.is_available():
        print("❌ Google Sheets недоступен")
        sys.exit(1)
    
    # Проверяем подключение к PostgreSQL
    from database_sync import _get_connection
    conn = _get_connection()
    if not conn:
        print("❌ PostgreSQL недоступен")
        sys.exit(1)
    conn.close()
    
    print("\n✅ Оба хранилища доступны")
    
    # Сравниваем и синхронизируем
    changes = False
    
    changes |= compare_and_sync_admins(sheets_manager)
    changes |= compare_and_sync_employees(sheets_manager)
    changes |= compare_and_sync_pending_employees(sheets_manager)
    changes |= compare_and_sync_default_schedule(sheets_manager)
    changes |= compare_and_sync_schedules(sheets_manager)
    changes |= compare_and_sync_requests(sheets_manager)
    changes |= compare_and_sync_queue(sheets_manager)
    
    print("\n" + "=" * 60)
    if changes:
        print("✅ Синхронизация завершена. Данные обновлены.")
    else:
        print("✅ Все данные идентичны. Синхронизация не требуется.")
    print("=" * 60)


if __name__ == "__main__":
    main()

