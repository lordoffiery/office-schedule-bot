"""
Скрипт для миграции всех данных из Google Sheets в PostgreSQL
"""
import asyncio
import os
import sys
import json
import logging
from datetime import datetime, date
from typing import Dict, List

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Устанавливаем переменные окружения
if not os.getenv('DATABASE_PUBLIC_URL') and not os.getenv('DATABASE_URL'):
    print("❌ DATABASE_URL не установлен!")
    print("Установите переменную окружения:")
    print("export DATABASE_PUBLIC_URL='postgresql://...'")
    sys.exit(1)

os.environ['BOT_TOKEN'] = os.getenv('BOT_TOKEN', 'migration_token')
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from config import (
    USE_GOOGLE_SHEETS, SHEET_EMPLOYEES, SHEET_ADMINS, SHEET_PENDING_EMPLOYEES,
    SHEET_SCHEDULES, SHEET_DEFAULT_SCHEDULE, SHEET_REQUESTS, SHEET_QUEUE, SHEET_LOGS
)
from database import (
    init_db, test_connection, close_db,
    save_admins_to_db, save_employee_to_db, save_pending_employee_to_db,
    save_schedule_to_db, save_default_schedule_to_db, save_request_to_db,
    add_to_queue_db, save_log_to_db
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


async def migrate_admins(sheets_manager: GoogleSheetsManager):
    """Мигрировать администраторов"""
    logger.info("📋 Миграция администраторов...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_ADMINS)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['admin_id', 'telegram_id', 'ID'])
        
        admin_ids = set()
        for row in rows[start_idx:]:
            if row and row[0]:
                try:
                    admin_id = int(row[0].strip())
                    admin_ids.add(admin_id)
                except ValueError:
                    continue
        
        if admin_ids:
            await save_admins_to_db(admin_ids)
            logger.info(f"✅ Мигрировано {len(admin_ids)} администраторов")
            return len(admin_ids)
        else:
            logger.warning("⚠️ Администраторы не найдены в Google Sheets")
            return 0
    except Exception as e:
        logger.error(f"❌ Ошибка миграции администраторов: {e}", exc_info=True)
        return 0


async def migrate_employees(sheets_manager: GoogleSheetsManager):
    """Мигрировать сотрудников"""
    logger.info("👥 Миграция сотрудников...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_EMPLOYEES)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['manual_name', 'telegram_name', 'telegram_id', 'username'])
        
        count = 0
        for row in rows[start_idx:]:
            if not row or len(row) < 3:
                continue
            
            try:
                # Формат: manual_name:telegram_name:telegram_id:username
                parts = row[0].split(':') if len(row) == 1 else [row[i] if i < len(row) else '' for i in range(4)]
                
                if len(parts) >= 3:
                    manual_name = parts[0].strip()
                    telegram_name = parts[1].strip() if len(parts) > 1 else ''
                    telegram_id = int(parts[2].strip()) if len(parts) > 2 and parts[2].strip() else None
                    username = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                    
                    if telegram_id and manual_name:
                        approved_by_admin = True  # Если в Google Sheets, значит одобрен
                        await save_employee_to_db(telegram_id, manual_name, telegram_name, username, approved_by_admin)
                        count += 1
            except (ValueError, IndexError) as e:
                logger.warning(f"⚠️ Пропущена строка сотрудника: {row} - {e}")
                continue
        
        logger.info(f"✅ Мигрировано {count} сотрудников")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции сотрудников: {e}", exc_info=True)
        return 0


async def migrate_pending_employees(sheets_manager: GoogleSheetsManager):
    """Мигрировать отложенных сотрудников"""
    logger.info("⏳ Миграция отложенных сотрудников...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_PENDING_EMPLOYEES)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['username', 'manual_name'])
        
        count = 0
        for row in rows[start_idx:]:
            if not row or len(row) < 2:
                continue
            
            try:
                username = row[0].strip() if row[0] else None
                manual_name = row[1].strip() if len(row) > 1 and row[1] else None
                
                if username and manual_name:
                    await save_pending_employee_to_db(username, manual_name)
                    count += 1
            except Exception as e:
                logger.warning(f"⚠️ Пропущена строка отложенного сотрудника: {row} - {e}")
                continue
        
        logger.info(f"✅ Мигрировано {count} отложенных сотрудников")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции отложенных сотрудников: {e}", exc_info=True)
        return 0


async def migrate_default_schedule(sheets_manager: GoogleSheetsManager):
    """Мигрировать расписание по умолчанию"""
    logger.info("📅 Миграция расписания по умолчанию...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['day_name', 'places_json'])
        
        schedule = {}
        for row in rows[start_idx:]:
            if not row or len(row) < 2:
                continue
            
            try:
                day_name = row[0].strip() if row[0] else None
                places_json_str = row[1].strip() if len(row) > 1 and row[1] else None
                
                if day_name and places_json_str:
                    try:
                        places_dict = json.loads(places_json_str)
                        schedule[day_name] = places_dict
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ Ошибка парсинга JSON для {day_name}: {places_json_str}")
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Пропущена строка расписания: {row} - {e}")
                continue
        
        if schedule:
            await save_default_schedule_to_db(schedule)
            logger.info(f"✅ Мигрировано расписание для {len(schedule)} дней")
            return len(schedule)
        else:
            logger.warning("⚠️ Расписание по умолчанию не найдено в Google Sheets")
            return 0
    except Exception as e:
        logger.error(f"❌ Ошибка миграции расписания по умолчанию: {e}", exc_info=True)
        return 0


async def migrate_schedules(sheets_manager: GoogleSheetsManager):
    """Мигрировать расписания на даты"""
    logger.info("📆 Миграция расписаний на даты...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['date', 'date_str', 'day_name', 'employees'])
        
        count = 0
        for row in rows[start_idx:]:
            if not row or len(row) < 3:
                continue
            
            try:
                date_str = row[0].strip() if row[0] else None
                day_name = row[1].strip() if len(row) > 1 and row[1] else None
                employees_str = row[2].strip() if len(row) > 2 and row[2] else None
                
                if date_str and day_name and employees_str:
                    # Проверяем формат даты
                    try:
                        datetime.strptime(date_str, '%Y-%m-%d')
                        await save_schedule_to_db(date_str, day_name, employees_str)
                        count += 1
                    except ValueError:
                        logger.warning(f"⚠️ Неверный формат даты: {date_str}")
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Пропущена строка расписания: {row} - {e}")
                continue
        
        logger.info(f"✅ Мигрировано {count} расписаний на даты")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции расписаний: {e}", exc_info=True)
        return 0


async def migrate_requests(sheets_manager: GoogleSheetsManager):
    """Мигрировать заявки на недели"""
    logger.info("📝 Миграция заявок на недели...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_REQUESTS)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['week_start', 'employee_name', 'telegram_id', 'days_requested', 'days_skipped'])
        
        count = 0
        for row in rows[start_idx:]:
            if not row or len(row) < 3:
                continue
            
            try:
                week_start_str = row[0].strip() if row[0] else None
                employee_name = row[1].strip() if len(row) > 1 and row[1] else None
                telegram_id_str = row[2].strip() if len(row) > 2 and row[2] else None
                days_requested_str = row[3].strip() if len(row) > 3 and row[3] else None
                days_skipped_str = row[4].strip() if len(row) > 4 and row[4] else None
                
                if week_start_str and employee_name and telegram_id_str:
                    try:
                        # Проверяем формат даты
                        datetime.strptime(week_start_str, '%Y-%m-%d')
                        telegram_id = int(telegram_id_str)
                        
                        days_requested = [d.strip() for d in days_requested_str.split(',')] if days_requested_str else []
                        days_skipped = [d.strip() for d in days_skipped_str.split(',')] if days_skipped_str else []
                        
                        await save_request_to_db(week_start_str, employee_name, telegram_id, days_requested, days_skipped)
                        count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Пропущена заявка: {row} - {e}")
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Пропущена строка заявки: {row} - {e}")
                continue
        
        logger.info(f"✅ Мигрировано {count} заявок")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции заявок: {e}", exc_info=True)
        return 0


async def migrate_queue(sheets_manager: GoogleSheetsManager):
    """Мигрировать очереди на дни"""
    logger.info("⏰ Миграция очередей на дни...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_QUEUE)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['date', 'employee_name', 'telegram_id'])
        
        count = 0
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
                        
                        await add_to_queue_db(date_str, employee_name, telegram_id)
                        count += 1
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Пропущена запись очереди: {row} - {e}")
                        continue
            except Exception as e:
                logger.warning(f"⚠️ Пропущена строка очереди: {row} - {e}")
                continue
        
        logger.info(f"✅ Мигрировано {count} записей очереди")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции очередей: {e}", exc_info=True)
        return 0


async def migrate_logs(sheets_manager: GoogleSheetsManager):
    """Мигрировать логи (опционально)"""
    logger.info("📊 Миграция логов...")
    
    try:
        rows = sheets_manager.read_all_rows(SHEET_LOGS)
        rows = filter_empty_rows(rows)
        start_idx, _ = get_header_start_idx(rows, ['timestamp', 'user_id', 'username', 'first_name', 'command', 'response'])
        
        count = 0
        max_logs = 10000  # Ограничиваем количество логов для миграции
        
        for row in rows[start_idx:start_idx + max_logs]:
            if not row or len(row) < 5:
                continue
            
            try:
                user_id_str = row[1].strip() if len(row) > 1 and row[1] else None
                username = row[2].strip() if len(row) > 2 and row[2] else None
                first_name = row[3].strip() if len(row) > 3 and row[3] else None
                command = row[4].strip() if len(row) > 4 and row[4] else None
                response = row[5].strip() if len(row) > 5 and row[5] else None
                
                if user_id_str and command:
                    try:
                        user_id = int(user_id_str)
                        await save_log_to_db(user_id, username or '', first_name or '', command, response or '')
                        count += 1
                    except ValueError:
                        continue
            except Exception as e:
                continue
        
        logger.info(f"✅ Мигрировано {count} логов (максимум {max_logs})")
        return count
    except Exception as e:
        logger.error(f"❌ Ошибка миграции логов: {e}", exc_info=True)
        return 0


async def main():
    """Основная функция миграции"""
    print("="*60)
    print("🚀 Миграция данных из Google Sheets в PostgreSQL")
    print("="*60)
    
    # Инициализация PostgreSQL
    print("\n1️⃣ Инициализация PostgreSQL...")
    success = await init_db()
    if not success:
        print("❌ Не удалось инициализировать PostgreSQL")
        return
    
    # Тест подключения
    print("\n2️⃣ Тест подключения...")
    if not await test_connection():
        print("❌ Ошибка подключения к PostgreSQL")
        await close_db()
        return
    
    # Инициализация Google Sheets
    print("\n3️⃣ Инициализация Google Sheets...")
    print("   Проверка переменных окружения и файлов...")
    
    credentials_env = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')
    spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
    
    if credentials_env:
        print("   ✅ GOOGLE_SHEETS_CREDENTIALS найден в переменных окружения")
    elif os.path.exists(credentials_file):
        print(f"   ✅ Файл {credentials_file} найден")
    else:
        print(f"   ❌ Не найдены credentials (ни в переменных окружения, ни в файле {credentials_file})")
        print("   Установите GOOGLE_SHEETS_CREDENTIALS или создайте файл google_credentials.json")
        await close_db()
        return
    
    if spreadsheet_id:
        print(f"   ✅ GOOGLE_SHEETS_ID найден: {spreadsheet_id}")
    else:
        print("   ❌ GOOGLE_SHEETS_ID не найден в переменных окружения")
        await close_db()
        return
    
    try:
        sheets_manager = GoogleSheetsManager()
        if not sheets_manager.is_available():
            print("❌ Google Sheets недоступен (проверьте credentials и права доступа)")
            await close_db()
            return
        print("✅ Google Sheets подключен")
    except Exception as e:
        print(f"❌ Ошибка подключения к Google Sheets: {e}")
        print("   Проверьте:")
        print("   1. Правильность GOOGLE_SHEETS_CREDENTIALS")
        print("   2. Наличие файла google_credentials.json")
        print("   3. Правильность GOOGLE_SHEETS_ID")
        print("   4. Права доступа Service Account к таблице")
        await close_db()
        return
    
    # Миграция данных
    print("\n4️⃣ Начало миграции данных...")
    print("-"*60)
    
    results = {}
    
    results['admins'] = await migrate_admins(sheets_manager)
    results['employees'] = await migrate_employees(sheets_manager)
    results['pending_employees'] = await migrate_pending_employees(sheets_manager)
    results['default_schedule'] = await migrate_default_schedule(sheets_manager)
    results['schedules'] = await migrate_schedules(sheets_manager)
    results['requests'] = await migrate_requests(sheets_manager)
    results['queue'] = await migrate_queue(sheets_manager)
    results['logs'] = await migrate_logs(sheets_manager)
    
    # Итоги
    print("\n" + "="*60)
    print("📊 Итоги миграции:")
    print("="*60)
    for key, value in results.items():
        print(f"  {key}: {value} записей")
    
    total = sum(results.values())
    print(f"\n✅ Всего мигрировано: {total} записей")
    
    # Закрытие подключения
    print("\n5️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Миграция завершена!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

