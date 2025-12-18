"""
Скрипт для синхронизации данных из PostgreSQL в Google Sheets
Запускается периодически (например, раз в час) для обновления Google Sheets
Google Sheets используется только как веб-интерфейс для просмотра и редактирования данных
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
os.environ['BOT_TOKEN'] = os.getenv('BOT_TOKEN', 'sync_token')
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from config import (
    USE_GOOGLE_SHEETS, SHEET_EMPLOYEES, SHEET_ADMINS, SHEET_PENDING_EMPLOYEES,
    SHEET_SCHEDULES, SHEET_DEFAULT_SCHEDULE, SHEET_REQUESTS, SHEET_QUEUE, SHEET_LOGS
)
from database_sync import (
    load_admins_from_db_sync, load_employees_from_db_sync, load_pending_employees_from_db_sync,
    load_default_schedule_from_db_sync, load_schedule_from_db_sync, load_requests_from_db_sync,
    load_queue_from_db_sync
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


def sync_admins_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать администраторов из PostgreSQL в Google Sheets"""
    print("\n👑 Синхронизация администраторов...")
    
    # Загружаем из PostgreSQL
    db_admins = load_admins_from_db_sync()
    
    if not db_admins:
        print("   ⚠️ В PostgreSQL нет администраторов")
        return
    
    # Формируем строки для Google Sheets
    rows = [['telegram_id']]  # Заголовок
    for admin_id in sorted(db_admins):
        rows.append([str(admin_id)])
    
    # Сохраняем в Google Sheets
    try:
        sheets_manager.write_rows(SHEET_ADMINS, rows, clear_first=True)
        print(f"   ✅ Синхронизировано {len(db_admins)} администраторов")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации администраторов: {e}")


def sync_employees_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать сотрудников из PostgreSQL в Google Sheets"""
    print("\n👥 Синхронизация сотрудников...")
    
    # Загружаем из PostgreSQL
    db_employees = load_employees_from_db_sync()
    
    if not db_employees:
        print("   ⚠️ В PostgreSQL нет сотрудников")
        return
    
    # Формируем строки для Google Sheets
    rows = [['manual_name', 'telegram_name', 'telegram_id', 'username']]  # Заголовок
    for telegram_id in sorted(db_employees.keys()):
        manual_name, telegram_name, username, approved = db_employees[telegram_id]
        username_str = username if username else ""
        rows.append([manual_name, telegram_name or manual_name, str(telegram_id), username_str])
    
    # Сохраняем в Google Sheets
    try:
        sheets_manager.write_rows(SHEET_EMPLOYEES, rows, clear_first=True)
        print(f"   ✅ Синхронизировано {len(db_employees)} сотрудников")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации сотрудников: {e}")


def sync_pending_employees_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать отложенных сотрудников из PostgreSQL в Google Sheets"""
    print("\n⏳ Синхронизация отложенных сотрудников...")
    
    # Загружаем из PostgreSQL
    db_pending = load_pending_employees_from_db_sync()
    
    if not db_pending:
        print("   ⚠️ В PostgreSQL нет отложенных сотрудников")
        # Очищаем Google Sheets
        try:
            sheets_manager.write_rows(SHEET_PENDING_EMPLOYEES, [['username', 'manual_name']], clear_first=True)
            print("   ✅ Google Sheets очищен")
        except Exception as e:
            print(f"   ❌ Ошибка очистки Google Sheets: {e}")
        return
    
    # Формируем строки для Google Sheets
    rows = [['username', 'manual_name']]  # Заголовок
    for username, manual_name in sorted(db_pending.items()):
        rows.append([username, manual_name])
    
    # Сохраняем в Google Sheets
    try:
        sheets_manager.write_rows(SHEET_PENDING_EMPLOYEES, rows, clear_first=True)
        print(f"   ✅ Синхронизировано {len(db_pending)} отложенных сотрудников")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации отложенных сотрудников: {e}")


def sync_default_schedule_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать расписание по умолчанию из PostgreSQL в Google Sheets"""
    print("\n📋 Синхронизация расписания по умолчанию...")
    
    # Загружаем из PostgreSQL
    db_schedule = load_default_schedule_from_db_sync()
    
    if not db_schedule:
        print("   ⚠️ В PostgreSQL нет расписания по умолчанию")
        return
    
    # Формируем строки для Google Sheets
    rows = [['day_name', 'places_json']]  # Заголовок
    for day_name in ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница']:
        places_dict = db_schedule.get(day_name, {})
        places_json = json.dumps(places_dict, ensure_ascii=False)
        rows.append([day_name, places_json])
    
    # Сохраняем в Google Sheets
    try:
        sheets_manager.write_rows(SHEET_DEFAULT_SCHEDULE, rows, clear_first=True)
        print(f"   ✅ Синхронизировано {len(db_schedule)} дней")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации расписания по умолчанию: {e}")


def sync_schedules_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать расписания из PostgreSQL в Google Sheets"""
    print("\n📅 Синхронизация расписаний...")
    
    # Загружаем все расписания из Google Sheets, чтобы получить список дат
    # (или можно загрузить из PostgreSQL напрямую, но проще через Google Sheets)
    try:
        existing_rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
        existing_rows = filter_empty_rows(existing_rows)
        start_idx, _ = get_header_start_idx(existing_rows, ['date', 'date_str', 'Дата'])
        
        # Собираем все даты из Google Sheets
        existing_dates = set()
        for row in existing_rows[start_idx:]:
            if row and row[0]:
                existing_dates.add(row[0].strip())
        
        # Также проверяем последние 60 дней на всякий случай
        from datetime import timedelta
        today = datetime.now().date()
        for i in range(60):
            date_str = (today + timedelta(days=i)).strftime('%Y-%m-%d')
            existing_dates.add(date_str)
        
        print(f"   Проверяю {len(existing_dates)} дат...")
        
        # Загружаем расписания из PostgreSQL для всех дат
        rows = [['date', 'day_name', 'employees']]  # Заголовок
        synced_count = 0
        
        for date_str in sorted(existing_dates):
            db_schedule = load_schedule_from_db_sync(date_str)
            if db_schedule:
                for day_name, employees_str in db_schedule.items():
                    rows.append([date_str, day_name, employees_str])
                    synced_count += 1
        
        # Сохраняем в Google Sheets
        if synced_count > 0:
            sheets_manager.write_rows(SHEET_SCHEDULES, rows, clear_first=True)
            print(f"   ✅ Синхронизировано {synced_count} расписаний")
        else:
            print("   ⚠️ Нет расписаний для синхронизации")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации расписаний: {e}")


def sync_requests_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать заявки из PostgreSQL в Google Sheets"""
    print("\n📝 Синхронизация заявок...")
    
    # Загружаем все заявки из Google Sheets, чтобы получить список недель
    try:
        existing_rows = sheets_manager.read_all_rows(SHEET_REQUESTS)
        existing_rows = filter_empty_rows(existing_rows)
        start_idx, _ = get_header_start_idx(existing_rows, ['week_start', 'week', 'Неделя', 'employee_name'])
        
        # Собираем все недели из Google Sheets
        existing_weeks = set()
        for row in existing_rows[start_idx:]:
            if row and row[0]:
                existing_weeks.add(row[0].strip())
        
        # Также проверяем последние 8 недель
        from datetime import timedelta
        today = datetime.now().date()
        for i in range(8):
            week_start = today - timedelta(days=today.weekday() + i * 7)
            week_str = week_start.strftime('%Y-%m-%d')
            existing_weeks.add(week_str)
        
        print(f"   Проверяю {len(existing_weeks)} недель...")
        
        # Загружаем заявки из PostgreSQL для всех недель
        rows = [['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped']]  # Заголовок
        synced_count = 0
        
        for week_str in sorted(existing_weeks):
            db_requests = load_requests_from_db_sync(week_str)
            if db_requests:
                for req in db_requests:
                    days_requested_str = ','.join(req['days_requested']) if req['days_requested'] else ''
                    days_skipped_str = ','.join(req['days_skipped']) if req['days_skipped'] else ''
                    rows.append([
                        week_str,
                        req['employee_name'],
                        str(req['telegram_id']),
                        days_requested_str,
                        days_skipped_str
                    ])
                    synced_count += 1
        
        # Сохраняем в Google Sheets
        if synced_count > 0:
            sheets_manager.write_rows(SHEET_REQUESTS, rows, clear_first=True)
            print(f"   ✅ Синхронизировано {synced_count} заявок")
        else:
            print("   ⚠️ Нет заявок для синхронизации")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации заявок: {e}")


def sync_queue_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать очередь из PostgreSQL в Google Sheets"""
    print("\n⏰ Синхронизация очереди...")
    
    # Загружаем все записи очереди из Google Sheets, чтобы получить список дат
    try:
        existing_rows = sheets_manager.read_all_rows(SHEET_QUEUE)
        existing_rows = filter_empty_rows(existing_rows)
        start_idx, _ = get_header_start_idx(existing_rows, ['date', 'date_str', 'Дата'])
        
        # Собираем все даты из Google Sheets
        existing_dates = set()
        for row in existing_rows[start_idx:]:
            if row and row[0]:
                existing_dates.add(row[0].strip())
        
        # Также проверяем последние 30 дней
        from datetime import timedelta
        today = datetime.now().date()
        for i in range(30):
            date_str = (today + timedelta(days=i)).strftime('%Y-%m-%d')
            existing_dates.add(date_str)
        
        print(f"   Проверяю {len(existing_dates)} дат...")
        
        # Загружаем очередь из PostgreSQL для всех дат
        rows = [['date', 'employee_name', 'telegram_id']]  # Заголовок
        synced_count = 0
        
        for date_str in sorted(existing_dates):
            db_queue = load_queue_from_db_sync(date_str)
            if db_queue:
                for entry in db_queue:
                    rows.append([
                        date_str,
                        entry['employee_name'],
                        str(entry['telegram_id'])
                    ])
                    synced_count += 1
        
        # Сохраняем в Google Sheets
        sheets_manager.write_rows(SHEET_QUEUE, rows, clear_first=True)
        print(f"   ✅ Синхронизировано {synced_count} записей в очереди")
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации очереди: {e}")


def sync_logs_to_sheets(sheets_manager: GoogleSheetsManager):
    """Синхронизировать логи из PostgreSQL в Google Sheets"""
    print("\n📝 Синхронизация логов...")
    
    try:
        from database_sync import _get_connection
        from psycopg2.extras import RealDictCursor
        
        conn = _get_connection()
        if not conn:
            print("   ⚠️ PostgreSQL недоступен, пропускаю синхронизацию логов")
            return
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Загружаем последние 1000 логов (чтобы не перегружать Google Sheets)
                cur.execute("""
                    SELECT timestamp, user_id, username, first_name, command, response
                    FROM logs
                    ORDER BY timestamp DESC
                    LIMIT 1000
                """)
                
                rows = cur.fetchall()
                
                if not rows:
                    print("   ⚠️ Нет логов для синхронизации")
                    return
                
                # Формируем строки для Google Sheets
                sheet_rows = [['timestamp', 'user_id', 'username', 'first_name', 'command', 'response']]  # Заголовок
                
                for row in reversed(rows):  # Переворачиваем, чтобы старые логи были первыми
                    sheet_rows.append([
                        row['timestamp'].strftime('%Y-%m-%d %H:%M:%S') if row['timestamp'] else '',
                        str(row['user_id']) if row['user_id'] else '',
                        row['username'] or '',
                        row['first_name'] or '',
                        row['command'] or '',
                        (row['response'] or '')[:500]  # Ограничиваем длину ответа
                    ])
                
                # Сохраняем в Google Sheets
                sheets_manager.write_rows(SHEET_LOGS, sheet_rows, clear_first=True)
                print(f"   ✅ Синхронизировано {len(rows)} логов")
        finally:
            conn.close()
    except Exception as e:
        print(f"   ❌ Ошибка синхронизации логов: {e}")


def main():
    """Основная функция"""
    print("=" * 60)
    print("🔄 Синхронизация данных")
    print("   PostgreSQL → Google Sheets")
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
    
    # Синхронизируем все данные
    sync_admins_to_sheets(sheets_manager)
    sync_employees_to_sheets(sheets_manager)
    sync_pending_employees_to_sheets(sheets_manager)
    sync_default_schedule_to_sheets(sheets_manager)
    sync_schedules_to_sheets(sheets_manager)
    sync_requests_to_sheets(sheets_manager)
    sync_queue_to_sheets(sheets_manager)
    sync_logs_to_sheets(sheets_manager)
    
    print("\n" + "=" * 60)
    print("✅ Синхронизация завершена")
    print("=" * 60)


if __name__ == "__main__":
    main()

