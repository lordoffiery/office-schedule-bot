"""
Тестовый скрипт для проверки подключения к PostgreSQL и создания таблиц
"""
import asyncio
import os
import sys
from database import init_db, test_connection, get_connection, close_db

# Устанавливаем DATABASE_URL из переменной окружения или используем предоставленную
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL')
if not DATABASE_URL:
    print("❌ DATABASE_URL не установлен!")
    print("Установите переменную окружения:")
    print("export DATABASE_PUBLIC_URL='postgresql://postgres:uceMHJlIrQoUnDOyZIzSEuadhbtRmWFI@metro.proxy.rlwy.net:15820/railway'")
    sys.exit(1)

# Временно устанавливаем для database.py
os.environ['DATABASE_PUBLIC_URL'] = DATABASE_URL

async def test_tables():
    """Проверить создание таблиц и их структуру"""
    print("\n" + "="*60)
    print("🔍 Проверка структуры таблиц")
    print("="*60)
    
    async with get_connection() as conn:
        # Получаем список всех таблиц
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        print(f"\n✅ Найдено таблиц: {len(tables)}")
        for table in tables:
            print(f"  - {table['table_name']}")
        
        # Проверяем структуру каждой таблицы
        expected_tables = [
            'employees', 'admins', 'pending_employees', 
            'schedules', 'default_schedule', 'requests', 
            'queue', 'logs'
        ]
        
        existing_tables = [t['table_name'] for t in tables]
        
        print("\n📋 Проверка наличия всех необходимых таблиц:")
        for table_name in expected_tables:
            if table_name in existing_tables:
                print(f"  ✅ {table_name}")
            else:
                print(f"  ❌ {table_name} - ОТСУТСТВУЕТ!")
        
        # Проверяем структуру ключевых таблиц
        print("\n🔍 Детальная проверка структуры таблиц:")
        
        # employees
        print("\n1. Таблица 'employees':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'employees'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # admins
        print("\n2. Таблица 'admins':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'admins'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # schedules
        print("\n3. Таблица 'schedules':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'schedules'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # default_schedule
        print("\n4. Таблица 'default_schedule':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'default_schedule'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # requests
        print("\n5. Таблица 'requests':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'requests'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # queue
        print("\n6. Таблица 'queue':")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'queue'
            ORDER BY ordinal_position
        """)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} ({nullable})")
        
        # Проверяем индексы
        print("\n📊 Проверка индексов:")
        indexes = await conn.fetch("""
            SELECT indexname, tablename
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        for idx in indexes:
            print(f"   - {idx['indexname']} на таблице {idx['tablename']}")


async def test_insert_select():
    """Тест записи и чтения данных"""
    print("\n" + "="*60)
    print("🧪 Тест записи и чтения данных")
    print("="*60)
    
    async with get_connection() as conn:
        # Тест 1: admins
        print("\n1. Тест таблицы 'admins':")
        test_admin_id = 999999999
        await conn.execute("INSERT INTO admins (telegram_id) VALUES ($1) ON CONFLICT (telegram_id) DO NOTHING", test_admin_id)
        result = await conn.fetchval("SELECT telegram_id FROM admins WHERE telegram_id = $1", test_admin_id)
        if result == test_admin_id:
            print(f"   ✅ Запись и чтение работают (admin_id: {result})")
            await conn.execute("DELETE FROM admins WHERE telegram_id = $1", test_admin_id)
        else:
            print(f"   ❌ Ошибка: ожидали {test_admin_id}, получили {result}")
        
        # Тест 2: employees
        print("\n2. Тест таблицы 'employees':")
        test_employee = {
            'telegram_id': 888888888,
            'manual_name': 'Тестовый Сотрудник',
            'telegram_name': 'Test User',
            'username': 'testuser',
            'approved_by_admin': True
        }
        await conn.execute("""
            INSERT INTO employees (telegram_id, manual_name, telegram_name, username, approved_by_admin)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (telegram_id) DO UPDATE SET
                manual_name = EXCLUDED.manual_name,
                telegram_name = EXCLUDED.telegram_name,
                username = EXCLUDED.username,
                approved_by_admin = EXCLUDED.approved_by_admin,
                updated_at = NOW()
        """, test_employee['telegram_id'], test_employee['manual_name'], 
            test_employee['telegram_name'], test_employee['username'], test_employee['approved_by_admin'])
        
        result = await conn.fetchrow("SELECT * FROM employees WHERE telegram_id = $1", test_employee['telegram_id'])
        if result and result['manual_name'] == test_employee['manual_name']:
            print(f"   ✅ Запись и чтение работают (employee: {result['manual_name']})")
            await conn.execute("DELETE FROM employees WHERE telegram_id = $1", test_employee['telegram_id'])
        else:
            print(f"   ❌ Ошибка при работе с employees")
        
        # Тест 3: schedules
        print("\n3. Тест таблицы 'schedules':")
        from datetime import date
        test_date = date(2099, 12, 31)
        test_employees = "Тестовый Сотрудник, Другой Сотрудник"
        await conn.execute("""
            INSERT INTO schedules (date, day_name, employees)
            VALUES ($1, $2, $3)
            ON CONFLICT (date) DO UPDATE SET
                day_name = EXCLUDED.day_name,
                employees = EXCLUDED.employees,
                updated_at = NOW()
        """, test_date, "Понедельник", test_employees)
        
        result = await conn.fetchrow("SELECT * FROM schedules WHERE date = $1", test_date)
        if result and result['employees'] == test_employees:
            print(f"   ✅ Запись и чтение работают (date: {result['date']}, employees: {result['employees']})")
            await conn.execute("DELETE FROM schedules WHERE date = $1", test_date)
        else:
            print(f"   ❌ Ошибка при работе с schedules")
        
        # Тест 4: default_schedule
        print("\n4. Тест таблицы 'default_schedule':")
        import json
        test_places = {"1.1": "Тестовый", "1.2": "Другой"}
        test_places_json = json.dumps(test_places, ensure_ascii=False)
        await conn.execute("""
            INSERT INTO default_schedule (day_name, places_json)
            VALUES ($1, $2)
            ON CONFLICT (day_name) DO UPDATE SET
                places_json = EXCLUDED.places_json,
                updated_at = NOW()
        """, "ТестовыйДень", test_places_json)
        
        result = await conn.fetchrow("SELECT * FROM default_schedule WHERE day_name = $1", "ТестовыйДень")
        if result:
            loaded_places = json.loads(result['places_json'])
            if loaded_places == test_places:
                print(f"   ✅ Запись и чтение JSON работают (places: {loaded_places})")
                await conn.execute("DELETE FROM default_schedule WHERE day_name = $1", "ТестовыйДень")
            else:
                print(f"   ❌ Ошибка: JSON не совпадает")
        else:
            print(f"   ❌ Ошибка при работе с default_schedule")


async def main():
    """Основная функция тестирования"""
    print("="*60)
    print("🚀 Тестирование подключения к PostgreSQL")
    print("="*60)
    print(f"\n📡 DATABASE_URL: {DATABASE_URL[:50]}...")
    
    # Инициализация
    print("\n1️⃣ Инициализация подключения...")
    success = await init_db()
    if not success:
        print("❌ Не удалось инициализировать подключение")
        return
    
    # Тест подключения
    print("\n2️⃣ Тест подключения...")
    if await test_connection():
        print("✅ Подключение работает!")
    else:
        print("❌ Ошибка подключения")
        await close_db()
        return
    
    # Проверка таблиц
    print("\n3️⃣ Проверка таблиц...")
    await test_tables()
    
    # Тест записи/чтения
    print("\n4️⃣ Тест записи и чтения...")
    await test_insert_select()
    
    # Закрытие
    print("\n5️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Все тесты завершены!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())

