"""
Модуль для работы с PostgreSQL базой данных
"""
import os
import logging
import asyncpg
import json
from typing import List, Dict, Optional, Set
from datetime import datetime
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# Получаем строку подключения из переменных окружения
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL')
if not DATABASE_URL:
    logger.warning("DATABASE_URL не установлен, PostgreSQL недоступен")

# Глобальный пул подключений
_pool: Optional[asyncpg.Pool] = None


async def init_db():
    """Инициализировать подключение к базе данных и создать таблицы"""
    global _pool
    
    logger.info("🔧 Инициализация PostgreSQL...")
    logger.info(f"   DATABASE_URL установлен: {DATABASE_URL is not None}")
    if DATABASE_URL:
        logger.info(f"   DATABASE_URL начинается с: {DATABASE_URL[:20]}..." if len(DATABASE_URL) > 20 else f"   DATABASE_URL: {DATABASE_URL}")
    
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL не установлен, пропускаем инициализацию БД")
        logger.warning("   Проверьте переменные окружения: DATABASE_URL или DATABASE_PUBLIC_URL")
        return False
    
    try:
        logger.info("   Создаю пул подключений к PostgreSQL...")
        # Создаем пул подключений
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=10,
            command_timeout=60
        )
        logger.info(f"   Пул подключений создан: {_pool is not None}")
        logger.info("✅ Подключение к PostgreSQL установлено")
        
        # Создаем таблицы
        logger.info("   Создаю/проверяю таблицы...")
        await create_tables()
        logger.info("✅ Таблицы в PostgreSQL созданы/проверены")
        
        logger.info(f"   Финальное состояние: _pool={_pool is not None}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к PostgreSQL: {e}", exc_info=True)
        logger.error(f"   _pool после ошибки: {_pool is not None}")
        _pool = None  # Убеждаемся, что _pool = None при ошибке
        return False


async def close_db():
    """Закрыть пул подключений"""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("Подключение к PostgreSQL закрыто")


@asynccontextmanager
async def get_connection():
    """Контекстный менеджер для получения подключения из пула"""
    if not _pool:
        raise RuntimeError("База данных не инициализирована. Вызовите init_db() сначала.")
    async with _pool.acquire() as connection:
        yield connection


async def create_tables():
    """Создать все необходимые таблицы"""
    async with get_connection() as conn:
        # Таблица сотрудников
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                telegram_id BIGINT PRIMARY KEY,
                manual_name TEXT NOT NULL,
                telegram_name TEXT,
                username TEXT,
                approved_by_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Таблица администраторов
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                telegram_id BIGINT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Таблица отложенных сотрудников
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_employees (
                username TEXT PRIMARY KEY,
                manual_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Таблица расписаний на даты
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                date DATE PRIMARY KEY,
                day_name TEXT NOT NULL,
                employees TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Таблица расписания по умолчанию
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS default_schedule (
                day_name TEXT PRIMARY KEY,
                places_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Таблица заявок на недели
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                week_start DATE NOT NULL,
                employee_name TEXT NOT NULL,
                telegram_id BIGINT NOT NULL,
                days_requested TEXT,
                days_skipped TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(week_start, telegram_id)
            )
        """)
        
        # Таблица очередей на дни
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                employee_name TEXT NOT NULL,
                telegram_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(date, telegram_id)
            )
        """)
        
        # Таблица логов (опционально)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT NOW(),
                user_id BIGINT,
                username TEXT,
                first_name TEXT,
                command TEXT,
                response TEXT
            )
        """)
        
        # Создаем индексы для ускорения запросов
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_week_start ON requests(week_start)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_requests_telegram_id ON requests(telegram_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_date ON queue(date)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_queue_telegram_id ON queue(telegram_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)")
        
        logger.info("Таблицы и индексы созданы/проверены")


async def test_connection() -> bool:
    """Протестировать подключение к базе данных"""
    if not DATABASE_URL:
        logger.warning("DATABASE_URL не установлен")
        return False
    
    try:
        async with get_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            if result == 1:
                logger.info("✅ Тест подключения к PostgreSQL успешен")
                return True
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка теста подключения к PostgreSQL: {e}")
        return False


# ============================================================================
# Функции для работы с администраторами
# ============================================================================

async def load_admins_from_db() -> Set[int]:
    """Загрузить всех администраторов из PostgreSQL"""
    if not _pool:
        return set()
    
    try:
        async with get_connection() as conn:
            rows = await conn.fetch("SELECT telegram_id FROM admins")
            return {row['telegram_id'] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка загрузки администраторов из PostgreSQL: {e}")
        return set()


async def save_admins_to_db(admin_ids: Set[int]) -> bool:
    """Сохранить администраторов в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            # Удаляем всех текущих админов
            await conn.execute("DELETE FROM admins")
            # Вставляем новых
            if admin_ids:
                await conn.executemany(
                    "INSERT INTO admins (telegram_id) VALUES ($1) ON CONFLICT (telegram_id) DO NOTHING",
                    [(admin_id,) for admin_id in admin_ids]
                )
            logger.debug(f"Сохранено {len(admin_ids)} администраторов в PostgreSQL")
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения администраторов в PostgreSQL: {e}")
        return False


async def add_admin_to_db(telegram_id: int) -> bool:
    """Добавить администратора в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute(
                "INSERT INTO admins (telegram_id) VALUES ($1) ON CONFLICT (telegram_id) DO NOTHING",
                telegram_id
            )
            return True
    except Exception as e:
        logger.error(f"Ошибка добавления администратора в PostgreSQL: {e}")
        return False


async def remove_admin_from_db(telegram_id: int) -> bool:
    """Удалить администратора из PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("DELETE FROM admins WHERE telegram_id = $1", telegram_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления администратора из PostgreSQL: {e}")
        return False


# ============================================================================
# Функции для работы с сотрудниками
# ============================================================================

async def load_employees_from_db() -> Dict[int, tuple]:
    """
    Загрузить всех сотрудников из PostgreSQL
    Returns: Dict[telegram_id, (manual_name, telegram_name, username, approved_by_admin)]
    """
    if not _pool:
        return {}
    
    try:
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT telegram_id, manual_name, telegram_name, username, approved_by_admin
                FROM employees
            """)
            result = {}
            for row in rows:
                result[row['telegram_id']] = (
                    row['manual_name'],
                    row['telegram_name'] or '',
                    row['username'] or None,
                    row['approved_by_admin'] or False
                )
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки сотрудников из PostgreSQL: {e}")
        return {}


async def save_employee_to_db(telegram_id: int, manual_name: str, telegram_name: str = None, 
                              username: str = None, approved_by_admin: bool = False) -> bool:
    """Сохранить/обновить сотрудника в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("""
                INSERT INTO employees (telegram_id, manual_name, telegram_name, username, approved_by_admin)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    manual_name = EXCLUDED.manual_name,
                    telegram_name = EXCLUDED.telegram_name,
                    username = EXCLUDED.username,
                    approved_by_admin = EXCLUDED.approved_by_admin,
                    updated_at = NOW()
            """, telegram_id, manual_name, telegram_name, username, approved_by_admin)
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения сотрудника в PostgreSQL: {e}")
        return False


async def load_pending_employees_from_db() -> Dict[str, str]:
    """Загрузить отложенных сотрудников из PostgreSQL"""
    if not _pool:
        return {}
    
    try:
        async with get_connection() as conn:
            rows = await conn.fetch("SELECT username, manual_name FROM pending_employees")
            return {row['username']: row['manual_name'] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка загрузки отложенных сотрудников из PostgreSQL: {e}")
        return {}


async def save_pending_employee_to_db(username: str, manual_name: str) -> bool:
    """Сохранить отложенного сотрудника в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("""
                INSERT INTO pending_employees (username, manual_name)
                VALUES ($1, $2)
                ON CONFLICT (username) DO UPDATE SET
                    manual_name = EXCLUDED.manual_name,
                    updated_at = NOW()
            """, username, manual_name)
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения отложенного сотрудника в PostgreSQL: {e}")
        return False


async def remove_pending_employee_from_db(username: str) -> bool:
    """Удалить отложенного сотрудника из PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("DELETE FROM pending_employees WHERE username = $1", username)
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления отложенного сотрудника из PostgreSQL: {e}")
        return False


# ============================================================================
# Функции для работы с расписаниями
# ============================================================================

async def load_schedule_from_db(date_str: str) -> Optional[Dict[str, str]]:
    """
    Загрузить расписание на дату из PostgreSQL
    Returns: {day_name: employees_str} или None
    """
    if not _pool:
        return None
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            schedule_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            schedule_date = date_str
        
        async with get_connection() as conn:
            row = await conn.fetchrow(
                "SELECT day_name, employees FROM schedules WHERE date = $1",
                schedule_date
            )
            if row:
                return {row['day_name']: row['employees']}
            return None
    except Exception as e:
        logger.error(f"Ошибка загрузки расписания из PostgreSQL: {e}")
        return None


async def save_schedule_to_db(date_str: str, day_name: str, employees_str: str) -> bool:
    """Сохранить расписание на дату в PostgreSQL"""
    if not _pool:
        logger.warning(f"⚠️ PostgreSQL pool недоступен, не могу сохранить расписание {date_str}")
        return False
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            schedule_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            schedule_date = date_str
        
        async with get_connection() as conn:
            await conn.execute("""
                INSERT INTO schedules (date, day_name, employees)
                VALUES ($1, $2, $3)
                ON CONFLICT (date) DO UPDATE SET
                    day_name = EXCLUDED.day_name,
                    employees = EXCLUDED.employees,
                    updated_at = NOW()
            """, schedule_date, day_name, employees_str)
            logger.debug(f"✅ Расписание {date_str} ({day_name}) успешно сохранено в PostgreSQL")
            return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения расписания {date_str} в PostgreSQL: {e}", exc_info=True)
        return False


async def load_default_schedule_from_db() -> Dict[str, Dict[str, str]]:
    """
    Загрузить расписание по умолчанию из PostgreSQL
    Returns: {day_name: {place: employee_name}}
    """
    if not _pool:
        return {}
    
    try:
        async with get_connection() as conn:
            rows = await conn.fetch("SELECT day_name, places_json FROM default_schedule")
            result = {}
            for row in rows:
                try:
                    places_dict = json.loads(row['places_json'])
                    result[row['day_name']] = places_dict
                except json.JSONDecodeError:
                    logger.warning(f"Ошибка парсинга JSON для {row['day_name']}")
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки расписания по умолчанию из PostgreSQL: {e}")
        return {}


async def save_default_schedule_to_db(schedule: Dict[str, Dict[str, str]]) -> bool:
    """Сохранить расписание по умолчанию в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            for day_name, places_dict in schedule.items():
                places_json = json.dumps(places_dict, ensure_ascii=False)
                await conn.execute("""
                    INSERT INTO default_schedule (day_name, places_json)
                    VALUES ($1, $2)
                    ON CONFLICT (day_name) DO UPDATE SET
                        places_json = EXCLUDED.places_json,
                        updated_at = NOW()
                """, day_name, places_json)
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения расписания по умолчанию в PostgreSQL: {e}")
        return False


async def load_requests_from_db(week_start_str: str) -> List[Dict]:
    """Загрузить заявки на неделю из PostgreSQL"""
    if not _pool:
        return []
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(week_start_str, str):
            week_start_date = datetime.strptime(week_start_str, '%Y-%m-%d').date()
        else:
            week_start_date = week_start_str
        
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT employee_name, telegram_id, days_requested, days_skipped
                FROM requests
                WHERE week_start = $1
            """, week_start_date)
            
            result = []
            for row in rows:
                days_requested = row['days_requested'].split(',') if row['days_requested'] else []
                days_skipped = row['days_skipped'].split(',') if row['days_skipped'] else []
                result.append({
                    'employee_name': row['employee_name'],
                    'telegram_id': row['telegram_id'],
                    'days_requested': [d.strip() for d in days_requested if d.strip()],
                    'days_skipped': [d.strip() for d in days_skipped if d.strip()]
                })
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки заявок из PostgreSQL: {e}")
        return []


async def save_request_to_db(week_start_str: str, employee_name: str, telegram_id: int,
                            days_requested: List[str], days_skipped: List[str]) -> bool:
    """Сохранить заявку на неделю в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(week_start_str, str):
            week_start_date = datetime.strptime(week_start_str, '%Y-%m-%d').date()
        else:
            week_start_date = week_start_str
        
        async with get_connection() as conn:
            days_requested_str = ','.join(days_requested) if days_requested else None
            days_skipped_str = ','.join(days_skipped) if days_skipped else None
            
            await conn.execute("""
                INSERT INTO requests (week_start, employee_name, telegram_id, days_requested, days_skipped)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (week_start, telegram_id) DO UPDATE SET
                    employee_name = EXCLUDED.employee_name,
                    days_requested = EXCLUDED.days_requested,
                    days_skipped = EXCLUDED.days_skipped,
                    updated_at = NOW()
            """, week_start_date, employee_name, telegram_id, days_requested_str, days_skipped_str)
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения заявки в PostgreSQL: {e}")
        return False


async def clear_requests_from_db(week_start_str: str) -> bool:
    """Очистить заявки на неделю из PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("DELETE FROM requests WHERE week_start = $1", week_start_str)
            return True
    except Exception as e:
        logger.error(f"Ошибка очистки заявок из PostgreSQL: {e}")
        return False


async def load_queue_from_db(date_str: str) -> List[Dict]:
    """Загрузить очередь на дату из PostgreSQL"""
    if not _pool:
        return []
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            queue_date = date_str
        
        async with get_connection() as conn:
            rows = await conn.fetch("""
                SELECT employee_name, telegram_id
                FROM queue
                WHERE date = $1
                ORDER BY created_at
            """, queue_date)
            
            return [
                {'employee_name': row['employee_name'], 'telegram_id': row['telegram_id']}
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Ошибка загрузки очереди из PostgreSQL: {e}")
        return []


async def add_to_queue_db(date_str: str, employee_name: str, telegram_id: int) -> bool:
    """Добавить в очередь на дату в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            queue_date = date_str
        
        async with get_connection() as conn:
            await conn.execute("""
                INSERT INTO queue (date, employee_name, telegram_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (date, telegram_id) DO NOTHING
            """, queue_date, employee_name, telegram_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка добавления в очередь в PostgreSQL: {e}")
        return False


async def remove_from_queue_db(date_str: str, telegram_id: int) -> bool:
    """Удалить из очереди на дату в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            queue_date = date_str
        
        async with get_connection() as conn:
            await conn.execute("DELETE FROM queue WHERE date = $1 AND telegram_id = $2", queue_date, telegram_id)
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления из очереди в PostgreSQL: {e}")
        return False


async def get_first_from_queue_db(date_str: str) -> Optional[Dict]:
    """Получить первого из очереди на дату из PostgreSQL"""
    if not _pool:
        return None
    
    try:
        # Конвертируем строку даты в объект date
        if isinstance(date_str, str):
            queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        else:
            queue_date = date_str
        
        async with get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT employee_name, telegram_id
                FROM queue
                WHERE date = $1
                ORDER BY created_at
                LIMIT 1
            """, queue_date)
            
            if row:
                return {'employee_name': row['employee_name'], 'telegram_id': row['telegram_id']}
            return None
    except Exception as e:
        logger.error(f"Ошибка получения первого из очереди в PostgreSQL: {e}")
        return None


# ============================================================================
# Функции для работы с логами
# ============================================================================

async def save_log_to_db(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Сохранить лог в PostgreSQL"""
    if not _pool:
        return False
    
    try:
        async with get_connection() as conn:
            await conn.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES ($1, $2, $3, $4, $5)
            """, user_id, username, first_name, command, response)
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL: {e}")
        return False

