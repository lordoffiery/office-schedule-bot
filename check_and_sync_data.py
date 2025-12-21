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
    logger.info("🔍 [ADMINS] Начало синхронизации администраторов")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_ADMINS)
    if not rows:
        print("⚠️ Google Sheets: администраторы не найдены")
        sheets_admins = set()
        logger.info("🔍 [ADMINS] Google Sheets: администраторы не найдены")
    else:
        sheets_admins = set()
        for row in rows:
            if row and row[0].strip():
                try:
                    admin_id = int(row[0].strip())
                    sheets_admins.add(admin_id)
                except ValueError:
                    continue
        logger.info(f"🔍 [ADMINS] Google Sheets: загружено {len(sheets_admins)} администраторов: {sorted(sheets_admins)}")
    
    # Загружаем из PostgreSQL
    db_admins = load_admins_from_db_sync()
    logger.info(f"🔍 [ADMINS] PostgreSQL: загружено {len(db_admins)} администраторов: {sorted(db_admins)}")
    
    print(f"   Google Sheets: {len(sheets_admins)} администраторов")
    print(f"   PostgreSQL: {len(db_admins)} администраторов")
    
    if sheets_admins != db_admins:
        only_in_sheets = sheets_admins - db_admins
        only_in_db = db_admins - sheets_admins
        print(f"   ⚠️ Различия найдены!")
        print(f"   Только в Google Sheets: {only_in_sheets}")
        print(f"   Только в PostgreSQL: {only_in_db}")
        logger.warning(f"⚠️ [ADMINS] Различия найдены! Только в Google Sheets: {only_in_sheets}, Только в PostgreSQL: {only_in_db}")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        print(f"   ⚠️ ВНИМАНИЕ: Администраторы из Google Sheets будут добавлены/обновлены")
        print(f"   ⚠️ Администраторы, которых нет в Google Sheets, НЕ будут удалены из PostgreSQL")
        logger.info(f"🔄 [ADMINS] Начинаю синхронизацию: clear_all=False (не удаляем существующих)")
        # НЕ используем clear_all=True, чтобы не удалять админов, которых нет в Google Sheets
        save_admins_to_db_sync(sheets_admins, clear_all=False)
        logger.info(f"✅ [ADMINS] Синхронизация завершена (добавлены/обновлены админы из Google Sheets)")
        print(f"   ✅ Синхронизация завершена (добавлены/обновлены админы из Google Sheets)")
        return True
    else:
        logger.info(f"✅ [ADMINS] Данные идентичны, синхронизация не требуется")
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_employees(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать сотрудников"""
    print("\n👥 Проверка сотрудников...")
    logger.info("🔍 [EMPLOYEES] Начало синхронизации сотрудников")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_EMPLOYEES)
    if not rows:
        print("⚠️ Google Sheets: сотрудники не найдены")
        sheets_employees = {}
        logger.info("🔍 [EMPLOYEES] Google Sheets: сотрудники не найдены")
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
        logger.info(f"🔍 [EMPLOYEES] Google Sheets: загружено {len(sheets_employees)} сотрудников")
    
    # Загружаем из PostgreSQL
    db_employees = load_employees_from_db_sync()
    logger.info(f"🔍 [EMPLOYEES] PostgreSQL: загружено {len(db_employees)} сотрудников")
    
    print(f"   Google Sheets: {len(sheets_employees)} сотрудников")
    print(f"   PostgreSQL: {len(db_employees)} сотрудников")
    
    differences = False
    # Проверяем различия
    all_ids = set(sheets_employees.keys()) | set(db_employees.keys())
    only_in_sheets = set(sheets_employees.keys()) - set(db_employees.keys())
    only_in_db = set(db_employees.keys()) - set(sheets_employees.keys())
    
    if only_in_sheets or only_in_db:
        logger.info(f"🔍 [EMPLOYEES] Только в Google Sheets: {len(only_in_sheets)} сотрудников")
        logger.info(f"🔍 [EMPLOYEES] Только в PostgreSQL: {len(only_in_db)} сотрудников")
    
    for telegram_id in all_ids:
        sheets_data = sheets_employees.get(telegram_id)
        db_data = db_employees.get(telegram_id)
        if sheets_data != db_data:
            differences = True
            logger.debug(f"🔍 [EMPLOYEES] Различия для telegram_id={telegram_id}: sheets={sheets_data}, db={db_data}")
            break
    
    if differences or len(sheets_employees) != len(db_employees):
        print(f"   ⚠️ Различия найдены!")
        logger.warning(f"⚠️ [EMPLOYEES] Различия найдены! Синхронизирую из Google Sheets в PostgreSQL")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        logger.info(f"🔄 [EMPLOYEES] Начинаю сохранение {len(sheets_employees)} сотрудников в PostgreSQL")
        for telegram_id, (manual_name, telegram_name, username, approved) in sheets_employees.items():
            logger.debug(f"💾 [EMPLOYEES] Сохранение сотрудника telegram_id={telegram_id}, name={manual_name}")
            save_employee_to_db_sync(telegram_id, manual_name, telegram_name, username, approved)
        logger.info(f"✅ [EMPLOYEES] Синхронизация завершена: сохранено {len(sheets_employees)} сотрудников")
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        logger.info(f"✅ [EMPLOYEES] Данные идентичны, синхронизация не требуется")
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_pending_employees(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать отложенных сотрудников"""
    logger.info("🔍 [PENDING_EMPLOYEES] Начало синхронизации отложенных сотрудников")
    print("\n⏳ Проверка отложенных сотрудников...")
    
    # Загружаем администраторов и сотрудников для проверки
    db_admins = load_admins_from_db_sync()
    db_employees = load_employees_from_db_sync()
    
    # Создаем словарь username -> telegram_id из сотрудников
    username_to_telegram_id = {}
    for telegram_id, (manual_name, telegram_name, username, approved) in db_employees.items():
        if username:
            username_to_telegram_id[username.lower()] = telegram_id
    
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
        skipped_admins = []
        for row in rows[header_idx + 1:]:
            if not row or len(row) < 2:
                continue
            username = row[0].strip().lower().lstrip('@')
            manual_name = row[1].strip() if len(row) > 1 else ''
            if username:
                # Проверяем, не является ли пользователь администратором
                telegram_id = username_to_telegram_id.get(username)
                if telegram_id and telegram_id in db_admins:
                    skipped_admins.append(username)
                    print(f"   ⚠️ Пропущен администратор @{username} (не должен быть в pending_employees)")
                    continue
                sheets_pending[username] = manual_name
        
        if skipped_admins:
            print(f"   ⚠️ Пропущено администраторов: {len(skipped_admins)}")
    
    # Загружаем из PostgreSQL
    db_pending = load_pending_employees_from_db_sync()
    
    # Удаляем администраторов из PostgreSQL pending_employees
    admins_in_pending = []
    for username in list(db_pending.keys()):
            telegram_id = username_to_telegram_id.get(username)
            if telegram_id and telegram_id in db_admins:
                admins_in_pending.append(username)
                logger.warning(f"🗑️ [PENDING_EMPLOYEES] DELETE: Удаление администратора @{username} (telegram_id={telegram_id}) из pending_employees")
                remove_pending_employee_from_db_sync(username)
                print(f"   🗑️ Удален администратор @{username} из pending_employees в PostgreSQL")
    
    if admins_in_pending:
        print(f"   🗑️ Удалено администраторов из PostgreSQL: {len(admins_in_pending)}")
        # Перезагружаем после удаления
        db_pending = load_pending_employees_from_db_sync()
    
    print(f"   Google Sheets: {len(sheets_pending)} отложенных сотрудников (после фильтрации администраторов)")
    print(f"   PostgreSQL: {len(db_pending)} отложенных сотрудников")
    
    if sheets_pending != db_pending:
        print(f"   ⚠️ Различия найдены!")
        print(f"   Только в Google Sheets: {set(sheets_pending.keys()) - set(db_pending.keys())}")
        print(f"   Только в PostgreSQL: {set(db_pending.keys()) - set(sheets_pending.keys())}")
        print(f"   🔄 Синхронизирую из Google Sheets в PostgreSQL...")
        # Удаляем тех, кого нет в Google Sheets
        for username in db_pending:
            if username not in sheets_pending:
                logger.warning(f"🗑️ [PENDING_EMPLOYEES] DELETE: Удаление @{username} из pending_employees (нет в Google Sheets)")
                remove_pending_employee_from_db_sync(username)
        # Добавляем/обновляем тех, кто есть в Google Sheets (уже без администраторов)
        for username, manual_name in sheets_pending.items():
            save_pending_employee_to_db_sync(username, manual_name)
        print(f"   ✅ Синхронизация завершена")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_default_schedule(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать расписание по умолчанию"""
    logger.info("🔍 [DEFAULT_SCHEDULE] Начало синхронизации расписания по умолчанию")
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
    logger.info("🔍 [SCHEDULES] Начало синхронизации расписаний")
    
    # Загружаем из Google Sheets
    rows = sheets_manager.read_all_rows(SHEET_SCHEDULES)
    if not rows:
        print("⚠️ Google Sheets: расписания не найдены")
        sheets_schedules = {}
        logger.info("🔍 [SCHEDULES] Google Sheets: расписания не найдены")
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
        logger.info(f"🔍 [SCHEDULES] Google Sheets: загружено {len(sheets_schedules)} расписаний для дат: {sorted(sheets_schedules.keys())[:10]}...")
    
    # Загружаем из PostgreSQL (проверяем все даты из Google Sheets)
    differences = False
    synced_count = 0
    
    logger.info(f"🔍 [SCHEDULES] Начинаю проверку {len(sheets_schedules)} расписаний из Google Sheets")
    for date_str in sheets_schedules:
        logger.debug(f"🔍 [SCHEDULES] Проверка даты {date_str}")
        db_schedule = load_schedule_from_db_sync(date_str)
        sheets_data = sheets_schedules[date_str]
        
        if db_schedule != sheets_data:
            differences = True
            print(f"   ⚠️ Различия для {date_str}:")
            print(f"      Google Sheets: {sheets_data}")
            print(f"      PostgreSQL: {db_schedule}")
            logger.warning(f"⚠️ [SCHEDULES] Различия для {date_str}: Google Sheets={sheets_data}, PostgreSQL={db_schedule}")
            # Синхронизируем
            logger.info(f"🔄 [SCHEDULES] Сохранение расписания для {date_str} из Google Sheets в PostgreSQL")
            for day_name, employees in sheets_data.items():
                logger.info(f"🔄 [SCHEDULES] Сохранение {date_str} ({day_name}): {employees[:100]}...")
                save_schedule_to_db_sync(date_str, day_name, employees)
            synced_count += 1
            logger.info(f"✅ [SCHEDULES] Расписание для {date_str} сохранено")
        else:
            logger.debug(f"✅ [SCHEDULES] Расписание для {date_str} идентично, пропускаем")
    
    print(f"   Google Sheets: {len(sheets_schedules)} расписаний")
    print(f"   PostgreSQL: проверено {len(sheets_schedules)} расписаний")
    
    if differences:
        logger.info(f"🔄 [SCHEDULES] Синхронизировано {synced_count} расписаний из {len(sheets_schedules)}")
        print(f"   🔄 Синхронизировано {synced_count} расписаний")
        return True
    else:
        logger.info(f"✅ [SCHEDULES] Все расписания идентичны, синхронизация не требуется")
        print(f"   ✅ Данные идентичны")
        return False


def compare_and_sync_requests(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать заявки"""
    logger.info("🔍 [REQUESTS] Начало синхронизации заявок")
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
    
    # Собираем все недели из Google Sheets
    weeks_in_sheets = set()
    for (week_start, _) in sheets_requests.keys():
        weeks_in_sheets.add(week_start)
    
    # Загружаем из PostgreSQL (по неделям из Google Sheets)
    differences = False
    synced_count = 0
    added_count = 0
    updated_count = 0
    deleted_count = 0
    
    # Сначала обрабатываем добавление и обновление заявок из Google Sheets
    for (week_start, telegram_id), sheets_data in sheets_requests.items():
        db_requests = load_requests_from_db_sync(week_start)
        db_data = None
        for req in db_requests:
            if req['telegram_id'] == telegram_id:
                db_data = req
                break
        
        # Нормализуем данные для сравнения
        sheets_days_requested = sorted(sheets_data.get('days_requested', []))
        sheets_days_skipped = sorted(sheets_data.get('days_skipped', []))
        sheets_employee_name = sheets_data.get('employee_name', '').strip()
        
        if db_data is None:
            # Заявки нет в PostgreSQL - добавляем
            differences = True
            added_count += 1
            logger.info(f"➕ [REQUESTS] Добавление новой заявки: неделя {week_start}, сотрудник {telegram_id} ({sheets_employee_name})")
            print(f"   ➕ Добавление новой заявки для недели {week_start}, сотрудник {telegram_id} ({sheets_employee_name})")
            save_request_to_db_sync(
                week_start,
                sheets_employee_name,
                sheets_data['telegram_id'],
                sheets_data['days_requested'],
                sheets_data['days_skipped']
            )
            synced_count += 1
        else:
            # Заявка есть в PostgreSQL - сравниваем
            db_days_requested = sorted(db_data.get('days_requested', []))
            db_days_skipped = sorted(db_data.get('days_skipped', []))
            db_employee_name = db_data.get('employee_name', '').strip()
            
            # Сравниваем данные
            if (db_days_requested != sheets_days_requested or 
                db_days_skipped != sheets_days_skipped or 
                db_employee_name != sheets_employee_name):
                differences = True
                updated_count += 1
                logger.info(f"🔄 [REQUESTS] Обновление заявки: неделя {week_start}, сотрудник {telegram_id} ({sheets_employee_name})")
                print(f"   🔄 Обновление заявки для недели {week_start}, сотрудник {telegram_id} ({sheets_employee_name})")
                print(f"      DB: запрошены={db_days_requested}, пропущены={db_days_skipped}")
                print(f"      Sheets: запрошены={sheets_days_requested}, пропущены={sheets_days_skipped}")
                save_request_to_db_sync(
                    week_start,
                    sheets_employee_name,
                    sheets_data['telegram_id'],
                    sheets_data['days_requested'],
                    sheets_data['days_skipped']
                )
                synced_count += 1
    
    # Теперь удаляем заявки из PostgreSQL, которых нет в Google Sheets
    # (только для недель, которые есть в Google Sheets)
    from database_sync import delete_request_from_db_sync
    for week_start in weeks_in_sheets:
        db_requests = load_requests_from_db_sync(week_start)
        sheets_telegram_ids = {telegram_id for (ws, telegram_id) in sheets_requests.keys() if ws == week_start}
        
        for db_req in db_requests:
            db_telegram_id = db_req.get('telegram_id')
            if db_telegram_id not in sheets_telegram_ids:
                # Заявка есть в PostgreSQL, но её нет в Google Sheets - удаляем
                differences = True
                deleted_count += 1
                db_employee_name = db_req.get('employee_name', '').strip()
                logger.info(f"🗑️ [REQUESTS] DELETE: Удаление заявки: неделя {week_start}, сотрудник {db_telegram_id} ({db_employee_name})")
                print(f"   🗑️ Удаление заявки для недели {week_start}, сотрудник {db_telegram_id} ({db_employee_name})")
                delete_request_from_db_sync(week_start, db_telegram_id)
                synced_count += 1
    
    print(f"   Google Sheets: {len(sheets_requests)} заявок")
    print(f"   PostgreSQL: проверено {len(sheets_requests)} заявок")
    
    if differences:
        print(f"   🔄 Синхронизировано {synced_count} заявок (добавлено: {added_count}, обновлено: {updated_count}, удалено: {deleted_count})")
        logger.info(f"✅ [REQUESTS] Синхронизация завершена: добавлено {added_count}, обновлено {updated_count}, удалено {deleted_count}")
        return True
    else:
        print(f"   ✅ Данные идентичны")
        logger.info(f"✅ [REQUESTS] Данные идентичны, синхронизация не требуется")
        return False


def compare_and_sync_queue(sheets_manager: GoogleSheetsManager):
    """Сравнить и синхронизировать очередь"""
    logger.info("🔍 [QUEUE] Начало синхронизации очереди")
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
            logger.warning(f"🗑️ [QUEUE] DELETE: Удаление всех записей очереди для {date_str} (будет синхронизировано из Google Sheets)")
            for q in db_queue:
                from database_sync import remove_from_queue_db_sync
                logger.debug(f"🗑️ [QUEUE] DELETE: Удаление записи date={date_str}, telegram_id={q['telegram_id']}, employee={q['employee_name']}")
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

