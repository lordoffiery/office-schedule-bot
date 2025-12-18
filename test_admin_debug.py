"""
Отладочный тест для проверки сохранения админа в PostgreSQL
"""
import asyncio
import os

os.environ['DATABASE_PUBLIC_URL'] = 'postgresql://postgres:uceMHJlIrQoUnDOyZIzSEuadhbtRmWFI@metro.proxy.rlwy.net:15820/railway'
os.environ['BOT_TOKEN'] = 'test'
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from database import init_db, test_connection, close_db, add_admin_to_db, load_admins_from_db, _pool

async def test():
    print("Инициализация...")
    await init_db()
    
    print(f"_pool доступен: {_pool is not None}")
    
    print("\nДобавляем админа напрямую через database...")
    result = await add_admin_to_db(999999999)
    print(f"Результат: {result}")
    
    print("\nПроверяем в БД...")
    admins = await load_admins_from_db()
    print(f"Админы в БД: {admins}")
    
    if 999999999 in admins:
        print("✅ Админ найден в БД!")
    else:
        print("❌ Админ НЕ найден в БД!")
    
    await close_db()

if __name__ == "__main__":
    asyncio.run(test())

