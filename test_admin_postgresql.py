"""
Тестовый скрипт для проверки работы admin_manager с PostgreSQL
"""
import asyncio
import os
import sys

# Устанавливаем переменные окружения
os.environ['DATABASE_PUBLIC_URL'] = 'postgresql://postgres:uceMHJlIrQoUnDOyZIzSEuadhbtRmWFI@metro.proxy.rlwy.net:15820/railway'
os.environ['BOT_TOKEN'] = 'test_token'  # Временный токен для теста
os.environ['USE_GOOGLE_SHEETS'] = 'true'  # Оставляем Google Sheets включенным

from database import init_db, test_connection, close_db, load_admins_from_db, save_admins_to_db
from admin_manager import AdminManager

async def test_admin_manager():
    """Тест работы AdminManager с PostgreSQL"""
    print("="*60)
    print("🧪 Тест AdminManager с PostgreSQL")
    print("="*60)
    
    # Инициализируем БД
    print("\n1️⃣ Инициализация PostgreSQL...")
    success = await init_db()
    if not success:
        print("❌ Не удалось инициализировать PostgreSQL")
        return
    
    # Тест подключения
    print("\n2️⃣ Тест подключения...")
    if await test_connection():
        print("✅ Подключение работает!")
    else:
        print("❌ Ошибка подключения")
        await close_db()
        return
    
    # Загружаем текущих админов из БД
    print("\n3️⃣ Загрузка админов из PostgreSQL...")
    db_admins = await load_admins_from_db()
    print(f"   Найдено админов в БД: {len(db_admins)}")
    for admin_id in sorted(db_admins):
        print(f"   - {admin_id}")
    
    # Тест AdminManager (создаем после инициализации БД)
    print("\n4️⃣ Тест AdminManager...")
    # Импортируем _pool и проверяем, что он инициализирован
    from database import _pool as db_pool
    print(f"   _pool доступен: {db_pool is not None}")
    
    admin_manager = AdminManager()
    print(f"   Админов загружено: {len(admin_manager.admins)}")
    for admin_id in sorted(admin_manager.admins):
        print(f"   - {admin_id}")
    
    # Тест добавления админа
    print("\n5️⃣ Тест добавления админа...")
    test_admin_id = 111111111
    
    # Проверяем, что админа нет в БД перед добавлением
    db_admins_before = await load_admins_from_db()
    print(f"   Админов в БД до добавления: {len(db_admins_before)}")
    
    # Тест прямого вызова add_admin_to_db
    print(f"\n   Тест прямого вызова add_admin_to_db...")
    from database import add_admin_to_db
    result = await add_admin_to_db(test_admin_id)
    print(f"   Результат add_admin_to_db: {result}")
    
    # Проверяем в БД после прямого вызова
    db_admins_after_direct = await load_admins_from_db()
    if test_admin_id in db_admins_after_direct:
        print(f"   ✅ Админ {test_admin_id} найден в PostgreSQL после прямого вызова")
    else:
        print(f"   ❌ Админ {test_admin_id} НЕ найден в PostgreSQL после прямого вызова")
    
    # Теперь тест через AdminManager
    print(f"\n   Тест через AdminManager.add_admin...")
    if admin_manager.add_admin(test_admin_id):
        print(f"   ✅ Админ {test_admin_id} добавлен в AdminManager")
    else:
        print(f"   ⚠️ Админ {test_admin_id} уже существует")
    
    # Ждем немного, чтобы async операции завершились
    import time
    time.sleep(2)
    
    # Проверяем в БД
    db_admins_after = await load_admins_from_db()
    print(f"   Админов в БД после добавления: {len(db_admins_after)}")
    if test_admin_id in db_admins_after:
        print(f"   ✅ Админ {test_admin_id} найден в PostgreSQL")
    else:
        print(f"   ❌ Админ {test_admin_id} НЕ найден в PostgreSQL")
        print(f"   Админы в БД: {sorted(db_admins_after)}")
    
    # Тест удаления админа
    print("\n6️⃣ Тест удаления админа...")
    if admin_manager.remove_admin(test_admin_id):
        print(f"   ✅ Админ {test_admin_id} удален")
    else:
        print(f"   ⚠️ Админ {test_admin_id} не был найден")
    
    # Проверяем в БД
    db_admins_final = await load_admins_from_db()
    if test_admin_id not in db_admins_final:
        print(f"   ✅ Админ {test_admin_id} удален из PostgreSQL")
    else:
        print(f"   ❌ Админ {test_admin_id} все еще в PostgreSQL")
    
    # Закрываем подключение
    print("\n7️⃣ Закрытие подключения...")
    await close_db()
    
    print("\n" + "="*60)
    print("✅ Все тесты завершены!")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(test_admin_manager())

