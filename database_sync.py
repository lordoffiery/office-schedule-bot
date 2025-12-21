"""
Синхронные функции для работы с PostgreSQL (для загрузки при старте бота)
Использует psycopg2 для избежания проблем с event loop
"""
import os
import logging
import psycopg2
import json
from typing import List, Dict, Optional, Set
from datetime import datetime
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# Получаем строку подключения из переменных окружения
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL')


def _get_connection():
    """Получить синхронное подключение к базе данных"""
    if not DATABASE_URL:
        return None
    try:
        # Преобразуем asyncpg URL в psycopg2 формат
        # postgresql://user:pass@host:port/db -> postgresql://user:pass@host:port/db
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL (sync): {e}")
        return None


def load_admins_from_db_sync() -> Set[int]:
    """Синхронная загрузка администраторов из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return set()
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT telegram_id FROM admins")
            rows = cur.fetchall()
            return {row['telegram_id'] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка загрузки администраторов из PostgreSQL (sync): {e}")
        return set()
    finally:
        conn.close()


def load_employees_from_db_sync() -> Dict[int, tuple]:
    """Синхронная загрузка сотрудников из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return {}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT telegram_id, manual_name, telegram_name, username, approved_by_admin
                FROM employees
            """)
            rows = cur.fetchall()
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
        logger.error(f"Ошибка загрузки сотрудников из PostgreSQL (sync): {e}")
        return {}
    finally:
        conn.close()


def load_pending_employees_from_db_sync() -> Dict[str, str]:
    """Синхронная загрузка отложенных сотрудников из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return {}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT username, manual_name FROM pending_employees")
            rows = cur.fetchall()
            return {row['username']: row['manual_name'] for row in rows}
    except Exception as e:
        logger.error(f"Ошибка загрузки отложенных сотрудников из PostgreSQL (sync): {e}")
        return {}
    finally:
        conn.close()


def load_default_schedule_from_db_sync() -> Dict[str, Dict[str, str]]:
    """Синхронная загрузка расписания по умолчанию из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return {}
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT day_name, places_json FROM default_schedule")
            rows = cur.fetchall()
            result = {}
            for row in rows:
                try:
                    places_dict = json.loads(row['places_json'])
                    result[row['day_name']] = places_dict
                except json.JSONDecodeError:
                    logger.warning(f"Ошибка парсинга JSON для {row['day_name']}")
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки расписания по умолчанию из PostgreSQL (sync): {e}")
        return {}
    finally:
        conn.close()


def load_schedule_from_db_sync(date_str: str) -> Optional[Dict[str, str]]:
    """Синхронная загрузка расписания на дату из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return None
    
    try:
        schedule_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT day_name, employees FROM schedules WHERE date = %s",
                (schedule_date,)
            )
            row = cur.fetchone()
            if row:
                return {row['day_name']: row['employees']}
            return None
    except Exception as e:
        logger.error(f"Ошибка загрузки расписания из PostgreSQL (sync): {e}")
        return None
    finally:
        conn.close()


def delete_schedule_from_db_sync(date_str: str) -> bool:
    """Синхронное удаление расписания на дату из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        schedule_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM schedules WHERE date = %s", (schedule_date,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления расписания из PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def load_requests_from_db_sync(week_start_str: str) -> List[Dict]:
    """Синхронная загрузка заявок из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return []
    
    try:
        week_start_date = datetime.strptime(week_start_str, "%Y-%m-%d").date()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT employee_name, telegram_id, days_requested, days_skipped
                FROM requests
                WHERE week_start = %s
            """, (week_start_date,))
            
            rows = cur.fetchall()
            result = []
            for row in rows:
                days_requested = row['days_requested'].split(',') if row['days_requested'] else []
                days_skipped = row['days_skipped'].split(',') if row['days_skipped'] else []
                result.append({
                    'employee_name': row['employee_name'],
                    'telegram_id': row['telegram_id'],
                    'days_requested': days_requested,
                    'days_skipped': days_skipped
                })
            return result
    except Exception as e:
        logger.error(f"Ошибка загрузки заявок из PostgreSQL (sync): {e}")
        return []
    finally:
        conn.close()


def load_queue_from_db_sync(date_str: str) -> List[Dict]:
    """Синхронная загрузка очереди из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return []
    
    try:
        queue_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT employee_name, telegram_id
                FROM queue
                WHERE date = %s
                ORDER BY created_at
            """, (queue_date,))
            
            rows = cur.fetchall()
            return [
                {
                    'employee_name': row['employee_name'],
                    'telegram_id': row['telegram_id']
                }
                for row in rows
            ]
    except Exception as e:
        logger.error(f"Ошибка загрузки очереди из PostgreSQL (sync): {e}")
        return []
    finally:
        conn.close()


def save_schedule_to_db_sync(date_str: str, day_name: str, employees_str: str) -> bool:
    """Синхронное сохранение расписания на дату в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        schedule_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO schedules (date, day_name, employees)
                VALUES (%s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    day_name = EXCLUDED.day_name,
                    employees = EXCLUDED.employees,
                    updated_at = NOW()
            """, (schedule_date, day_name, employees_str))
            conn.commit()
            logger.debug(f"✅ Расписание {date_str} ({day_name}) успешно сохранено в PostgreSQL (sync)")
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения расписания в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def remove_from_queue_db_sync(date_str: str, telegram_id: int) -> bool:
    """Синхронное удаление из очереди на дату в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM queue WHERE date = %s AND telegram_id = %s", (queue_date, telegram_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления из очереди в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def add_to_queue_db_sync(date_str: str, employee_name: str, telegram_id: int) -> bool:
    """Синхронное добавление в очередь на дату в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        queue_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO queue (date, employee_name, telegram_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (date, telegram_id) DO NOTHING
            """, (queue_date, employee_name, telegram_id))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка добавления в очередь в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_default_schedule_to_db_sync(schedule: Dict[str, Dict[str, str]]) -> bool:
    """Синхронное сохранение расписания по умолчанию в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            for day_name, places_dict in schedule.items():
                places_json = json.dumps(places_dict, ensure_ascii=False)
                cur.execute("""
                    INSERT INTO default_schedule (day_name, places_json)
                    VALUES (%s, %s)
                    ON CONFLICT (day_name) DO UPDATE SET
                        places_json = EXCLUDED.places_json,
                        updated_at = NOW()
                """, (day_name, places_json))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения расписания по умолчанию в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_request_to_db_sync(week_start_str: str, employee_name: str, telegram_id: int,
                            days_requested: List[str], days_skipped: List[str]) -> bool:
    """Синхронное сохранение заявки на неделю в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        week_start_date = datetime.strptime(week_start_str, '%Y-%m-%d').date()
        days_requested_str = ','.join(days_requested) if days_requested else None
        days_skipped_str = ','.join(days_skipped) if days_skipped else None
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO requests (week_start, employee_name, telegram_id, days_requested, days_skipped)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (week_start, telegram_id) DO UPDATE SET
                    employee_name = EXCLUDED.employee_name,
                    days_requested = EXCLUDED.days_requested,
                    days_skipped = EXCLUDED.days_skipped,
                    updated_at = NOW()
            """, (week_start_date, employee_name, telegram_id, days_requested_str, days_skipped_str))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения заявки в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def clear_requests_from_db_sync(week_start_str: str) -> bool:
    """Синхронная очистка заявок на неделю из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        week_start_date = datetime.strptime(week_start_str, '%Y-%m-%d').date()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM requests WHERE week_start = %s", (week_start_date,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка очистки заявок в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_employee_to_db_sync(telegram_id: int, manual_name: str, telegram_name: str = None, 
                             username: str = None, approved_by_admin: bool = False) -> bool:
    """Синхронное сохранение/обновление сотрудника в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO employees (telegram_id, manual_name, telegram_name, username, approved_by_admin)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (telegram_id) DO UPDATE SET
                    manual_name = EXCLUDED.manual_name,
                    telegram_name = EXCLUDED.telegram_name,
                    username = EXCLUDED.username,
                    approved_by_admin = EXCLUDED.approved_by_admin,
                    updated_at = NOW()
            """, (telegram_id, manual_name, telegram_name or '', username, approved_by_admin))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения сотрудника в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_admins_to_db_sync(admin_ids: Set[int], clear_all: bool = False) -> bool:
    """
    Синхронное сохранение администраторов в PostgreSQL
    
    Args:
        admin_ids: Множество ID администраторов для сохранения
        clear_all: Если True, удаляет всех админов перед добавлением (опасно!)
                   Если False, только добавляет/обновляет указанных админов
    """
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            if clear_all:
                # Удаляем всех текущих админов (используется только при полной синхронизации)
                cur.execute("DELETE FROM admins")
                logger.warning("⚠️ ВНИМАНИЕ: Все администраторы удалены перед синхронизацией!")
            
            # Вставляем/обновляем указанных админов
            if admin_ids:
                cur.executemany(
                    "INSERT INTO admins (telegram_id) VALUES (%s) ON CONFLICT (telegram_id) DO NOTHING",
                    [(admin_id,) for admin_id in admin_ids]
                )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения администраторов в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def add_admin_to_db_sync(telegram_id: int) -> bool:
    """Синхронное добавление администратора в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO admins (telegram_id) VALUES (%s) ON CONFLICT (telegram_id) DO NOTHING",
                (telegram_id,)
            )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка добавления администратора в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def remove_admin_from_db_sync(telegram_id: int) -> bool:
    """Синхронное удаление администратора из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM admins WHERE telegram_id = %s", (telegram_id,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления администратора из PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_pending_employee_to_db_sync(username: str, manual_name: str) -> bool:
    """Синхронное сохранение отложенного сотрудника в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pending_employees (username, manual_name)
                VALUES (%s, %s)
                ON CONFLICT (username) DO UPDATE SET
                    manual_name = EXCLUDED.manual_name,
                    updated_at = NOW()
            """, (username, manual_name))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения отложенного сотрудника в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def save_log_to_db_sync(user_id: int, username: str, first_name: str, command: str, response: str) -> bool:
    """Синхронное сохранение лога в PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO logs (user_id, username, first_name, command, response)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, username, first_name, command, response))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения лога в PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def remove_pending_employee_from_db_sync(username: str) -> bool:
    """Синхронное удаление отложенного сотрудника из PostgreSQL"""
    conn = _get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pending_employees WHERE username = %s", (username,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления отложенного сотрудника из PostgreSQL (sync): {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
