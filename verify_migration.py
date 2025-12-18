"""
Скрипт для проверки миграции данных из Google Sheets в PostgreSQL
"""
import asyncio
import os
import sys

os.environ['DATABASE_PUBLIC_URL'] = os.getenv('DATABASE_PUBLIC_URL', 'postgresql://postgres:uceMHJlIrQoUnDOyZIzSEuadhbtRmWFI@metro.proxy.rlwy.net:15820/railway')
os.environ['BOT_TOKEN'] = 'test'

from database import (
    init_db, test_connection, close_db,
    load_admins_from_db, load_employees_from_db, load_pending_employees_from_db,
    load_default_schedule_from_db, load_schedule_from_db, load_requests_from_db,
    load_queue_from_db
)

async def verify_migration():
    """Проверить миграцию данных"""
    print("="*60)
    print("🔍 Проверка миграции данных")
    print("="*60)
    
    # Инициализация
    print("\n1️⃣ Инициализация PostgreSQL...")
    success = await init_db()
    if not success:
        print("❌ Не удалось инициализировать PostgreSQL")
        return
    
    if not await test_connection():
        print("❌ Ошибка подключения")
        await close_db()
        return
    
    # Проверка данных
    print("\n2️⃣ Проверка данных в PostgreSQL...")
    
    # Администраторы
    admins = await load_admins_from_db()
    print(f"\n📋 Администраторы: {len(admins)} записей")
    for admin_id in sorted(admins)[:5]:
        print(f"   - {admin_id}")
    if len(admins) > 5:
        print(f"   ... и еще {len(admins) - 5}")
    
    # Сотрудники
    employees = await load_employees_from_db()
    print(f"\n👥 Сотрудники: {len(employees)} записей")
    for telegram_id, (manual_name, telegram_name, username, approved) in list(employees.items())[:5]:
        print(f"   - {manual_name} ({telegram_id}) - одобрен: {approved}")
    if len(employees) > 5:
        print(f"   ... и еще {len(employees) - 5}")
    
    # Отложенные сотрудники
    pending = await load_pending_employees_from_db()
    print(f"\n⏳ Отложенные сотрудники: {len(pending)} записей")
    for username, manual_name in list(pending.items())[:5]:
        print(f"   - {username} -> {manual_name}")
    
    # Расписание по умолчанию
    default_schedule = await load_default_schedule_from_db()
    print(f"\n📅 Расписание по умолчанию: {len(default_schedule)} дней")
    for day_name, places in list(default_schedule.items())[:2]:
        print(f"   - {day_name}: {len(places)} мест")
        for place, name in list(places.items())[:3]:
            print(f"     {place}: {name}")
    
    # Расписания на даты
    print(f"\n📆 Расписания на даты:")
    # Проверяем несколько дат
    test_dates = ['2025-12-16', '2025-12-18', '2025-12-22']
    for date_str in test_dates:
        schedule = await load_schedule_from_db(date_str)
        if schedule:
            for day_name, employees_str in schedule.items():
                emp_count = len([e for e in employees_str.split(',') if e.strip()])
                print(f"   - {date_str} ({day_name}): {emp_count} сотрудников")
        else:
            print(f"   - {date_str}: нет данных")
    
    # Заявки
    print(f"\n📝 Заявки:")
    # Проверяем несколько недель
    test_weeks = ['2025-12-22', '2025-12-29', '2026-12-14']
    for week_str in test_weeks:
        requests = await load_requests_from_db(week_str)
        if requests:
            print(f"   - {week_str}: {len(requests)} заявок")
            for req in requests[:2]:
                print(f"     {req['employee_name']}: запрошено {len(req['days_requested'])}, пропущено {len(req['days_skipped'])}")
        else:
            print(f"   - {week_str}: нет данных")
    
    # Очереди
    print(f"\n⏰ Очереди:")
    test_queue_dates = ['2025-12-18', '2025-12-19']
    for date_str in test_queue_dates:
        queue = await load_queue_from_db(date_str)
        if queue:
            print(f"   - {date_str}: {len(queue)} в очереди")
        else:
            print(f"   - {date_str}: нет данных")
    
    # Закрытие
    print("\n3️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Проверка завершена!")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(verify_migration())

