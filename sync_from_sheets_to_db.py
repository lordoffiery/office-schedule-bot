"""
Скрипт для синхронизации данных из Google Sheets в PostgreSQL
Синхронизирует только те записи, которые различаются
"""
import asyncio
import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List

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
    USE_GOOGLE_SHEETS, SHEET_SCHEDULES, SHEET_REQUESTS, SHEET_QUEUE
)
from database import (
    init_db, test_connection, close_db,
    save_schedule_to_db, save_request_to_db, add_to_queue_db
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


async def sync_schedules(sheets_manager: GoogleSheetsManager):
    """Синхронизировать расписания из Google Sheets в PostgreSQL"""
    print("\n📆 Синхронизация расписаний...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'day_name', 'employees'])
    
    synced_count = 0
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            date_str = row[0].strip() if row[0] else None
            day_name = row[1].strip() if len(row) > 1 and row[1] else None
            employees_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if date_str and day_name and employees_str:
                try:
                    # Проверяем формат даты
                    datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # Сохраняем в PostgreSQL
                    success = await save_schedule_to_db(date_str, day_name, employees_str)
                    if success:
                        synced_count += 1
                        print(f"   ✅ Синхронизировано расписание для {date_str} ({day_name})")
                    else:
                        print(f"   ❌ Ошибка синхронизации расписания для {date_str}")
                except ValueError:
                    continue
        except Exception as e:
            logger.error(f"Ошибка обработки строки расписания: {e}")
            continue
    
    print(f"   📊 Всего синхронизировано расписаний: {synced_count}")
    return synced_count


async def sync_requests(sheets_manager: GoogleSheetsManager):
    """Синхронизировать заявки из Google Sheets в PostgreSQL"""
    print("\n📝 Синхронизация заявок...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_REQUESTS)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['week_start', 'week', 'Неделя', 'employee_name'])
    
    synced_count = 0
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            week_start_str = row[0].strip() if row[0] else None
            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
            telegram_id_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if week_start_str and employee_name and telegram_id_str:
                try:
                    # Проверяем формат даты
                    datetime.strptime(week_start_str, '%Y-%m-%d')
                    telegram_id = int(telegram_id_str)
                    
                    days_requested_str = row[3].strip() if len(row) > 3 and row[3] else None
                    days_skipped_str = row[4].strip() if len(row) > 4 and row[4] else None
                    
                    days_requested = [d.strip() for d in days_requested_str.split(',')] if days_requested_str else []
                    days_skipped = [d.strip() for d in days_skipped_str.split(',')] if days_skipped_str else []
                    
                    # Удаляем пустые строки
                    days_requested = [d for d in days_requested if d]
                    days_skipped = [d for d in days_skipped if d]
                    
                    # Сохраняем в PostgreSQL
                    success = await save_request_to_db(
                        week_start_str, employee_name, telegram_id,
                        days_requested, days_skipped
                    )
                    if success:
                        synced_count += 1
                        print(f"   ✅ Синхронизирована заявка: {employee_name} (неделя {week_start_str})")
                        if days_requested:
                            print(f"      Запрошено: {', '.join(days_requested)}")
                        if days_skipped:
                            print(f"      Пропущено: {', '.join(days_skipped)}")
                    else:
                        print(f"   ❌ Ошибка синхронизации заявки для {employee_name} (неделя {week_start_str})")
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка парсинга заявки: {e}")
                    continue
        except Exception as e:
            logger.error(f"Ошибка обработки строки заявки: {e}")
            continue
    
    print(f"   📊 Всего синхронизировано заявок: {synced_count}")
    return synced_count


async def sync_queue(sheets_manager: GoogleSheetsManager):
    """Синхронизировать очередь из Google Sheets в PostgreSQL"""
    print("\n⏳ Синхронизация очереди...")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_QUEUE)
    rows = filter_empty_rows(rows)
    start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'Дата', 'employee_name'])
    
    synced_count = 0
    for row in rows[start_idx:]:
        if not row or len(row) < 3:
            continue
        
        try:
            date_str = row[0].strip() if row[0] else None
            employee_name = row[1].strip() if len(row) > 1 and row[1] else None
            telegram_id_str = row[2].strip() if len(row) > 2 and row[2] else None
            
            if date_str and employee_name and telegram_id_str:
                try:
                    # Проверяем формат даты
                    datetime.strptime(date_str, '%Y-%m-%d')
                    telegram_id = int(telegram_id_str)
                    
                    # Сохраняем в PostgreSQL
                    success = await add_to_queue_db(date_str, employee_name, telegram_id)
                    if success:
                        synced_count += 1
                        print(f"   ✅ Синхронизирована очередь: {employee_name} на {date_str}")
                    else:
                        print(f"   ❌ Ошибка синхронизации очереди для {employee_name} на {date_str}")
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка парсинга очереди: {e}")
                    continue
        except Exception as e:
            logger.error(f"Ошибка обработки строки очереди: {e}")
            continue
    
    print(f"   📊 Всего синхронизировано записей очереди: {synced_count}")
    return synced_count


async def main():
    """Основная функция синхронизации"""
    print("="*60)
    print("🔄 Синхронизация данных из Google Sheets в PostgreSQL")
    print("="*60)
    
    # Инициализация PostgreSQL
    print("\n1️⃣ Инициализация PostgreSQL...")
    use_postgresql = await init_db()
    if not use_postgresql:
        print("❌ PostgreSQL недоступен")
        return
    
    await test_connection()
    print("✅ PostgreSQL готов")
    
    # Инициализация Google Sheets
    print("\n2️⃣ Инициализация Google Sheets...")
    sheets_manager = GoogleSheetsManager()
    if not sheets_manager.is_available():
        print("❌ Google Sheets недоступен")
        await close_db()
        return
    print("✅ Google Sheets готов")
    
    # Синхронизация
    print("\n3️⃣ Синхронизация данных...")
    print("-" * 60)
    
    schedules_count = await sync_schedules(sheets_manager)
    requests_count = await sync_requests(sheets_manager)
    queue_count = await sync_queue(sheets_manager)
    
    print("\n" + "="*60)
    print("📊 Итоги синхронизации:")
    print("="*60)
    print(f"  ✅ Расписаний синхронизировано: {schedules_count}")
    print(f"  ✅ Заявок синхронизировано: {requests_count}")
    print(f"  ✅ Записей очереди синхронизировано: {queue_count}")
    print("="*60)
    
    # Закрытие подключения
    print("\n4️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Синхронизация завершена!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

