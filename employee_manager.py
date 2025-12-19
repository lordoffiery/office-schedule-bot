"""
Управление сотрудниками
"""
import os
import logging
import asyncio
from typing import Dict, Optional, List, Tuple
from config import (
    EMPLOYEES_FILE, DATA_DIR, PENDING_EMPLOYEES_FILE,
    USE_GOOGLE_SHEETS, USE_GOOGLE_SHEETS_FOR_WRITES, USE_GOOGLE_SHEETS_FOR_READS,
    SHEET_EMPLOYEES, SHEET_PENDING_EMPLOYEES, USE_POSTGRESQL
)
from utils import get_header_start_idx, filter_empty_rows

# Настройка логирования
logger = logging.getLogger(__name__)

# Импортируем Google Sheets Manager только если нужно
if USE_GOOGLE_SHEETS:
    try:
        from google_sheets_manager import GoogleSheetsManager
    except ImportError:
        GoogleSheetsManager = None
else:
    GoogleSheetsManager = None

# Импортируем функции для работы с PostgreSQL
if USE_POSTGRESQL:
    try:
        from database import (
            load_employees_from_db, save_employee_to_db,
            load_pending_employees_from_db, save_pending_employee_to_db,
            remove_pending_employee_from_db
        )
    except ImportError:
        load_employees_from_db = None
        save_employee_to_db = None
        load_pending_employees_from_db = None
        save_pending_employee_to_db = None
        remove_pending_employee_from_db = None
else:
    load_employees_from_db = None
    save_employee_to_db = None
    load_pending_employees_from_db = None
    save_pending_employee_to_db = None
    remove_pending_employee_from_db = None


def _get_pool():
    """Получить пул подключений PostgreSQL (динамический импорт)"""
    if not USE_POSTGRESQL:
        return None
    try:
        from database import _pool
        return _pool
    except ImportError:
        return None


class EmployeeManager:
    """Класс для управления списком сотрудников"""
    
    def __init__(self):
        # Формат: telegram_id -> (имя_вручную, имя_из_телеги, никнейм)
        self.employees: Dict[int, Tuple[str, str, Optional[str]]] = {}
        # Для обратного поиска: имя_вручную -> telegram_id
        self.name_to_id: Dict[str, int] = {}
        # Отложенные записи: username -> manual_name (для случаев, когда админ добавляет по username до /start)
        self.pending_employees: Dict[str, str] = {}
        # Флаг одобрения админом: telegram_id -> bool (True если был добавлен админом)
        self.approved_by_admin: Dict[int, bool] = {}
        
        # Инициализируем Google Sheets Manager если нужно
        self.sheets_manager = None
        if USE_GOOGLE_SHEETS and GoogleSheetsManager:
            try:
                self.sheets_manager = GoogleSheetsManager()
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Google Sheets: {e}")
        
        self._load_employees()
        self._load_pending_employees()
    
    def _load_employees(self):
        """Загрузить список сотрудников из PostgreSQL (приоритет), Google Sheets или файла"""
        # Очищаем текущие данные
        self.employees = {}
        self.name_to_id = {}
        self.approved_by_admin = {}
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки при старте
        if USE_POSTGRESQL:
            try:
                from database_sync import load_employees_from_db_sync
                logger.debug("Используем синхронную загрузку сотрудников из PostgreSQL")
                db_employees = load_employees_from_db_sync()
                logger.debug("load_employees_from_db_sync завершен успешно")
            except Exception as e:
                logger.warning(f"Ошибка загрузки сотрудников из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
                db_employees = None
            
            if db_employees:
                for telegram_id, (manual_name, telegram_name, username, approved) in db_employees.items():
                    self.employees[telegram_id] = (manual_name, telegram_name, username)
                    self.name_to_id[manual_name] = telegram_id
                    self.approved_by_admin[telegram_id] = approved
                logger.info(f"Сотрудники загружены из PostgreSQL: {len(self.employees)} записей")
                # Сохраняем в файл для совместимости
                self._save_employees_to_file_only()
                # Синхронизируем с Google Sheets
                self._sync_employees_to_google_sheets()
                return
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и PostgreSQL недоступен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            # Проверяем, есть ли буферизованные операции для листа employees
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_EMPLOYEES)
            
            if not has_buffered:
                try:
                    rows = self.sheets_manager.read_all_rows(SHEET_EMPLOYEES)
                    rows = filter_empty_rows(rows)
                    start_idx, _ = get_header_start_idx(rows, ['manual_name', 'Имя вручную'])
                    loaded_from_sheets = False
                    for row in rows[start_idx:]:
                        if len(row) < 3 or not row[0] or not row[2]:
                            continue
                        try:
                            manual_name = row[0].strip()
                            telegram_name = row[1].strip() if len(row) > 1 and row[1].strip() else manual_name
                            telegram_id = int(row[2].strip())
                            username = row[3].strip() if len(row) > 3 and row[3].strip() else None
                            
                            self.employees[telegram_id] = (manual_name, telegram_name, username)
                            self.name_to_id[manual_name] = telegram_id
                            # Если загружаем из Google Sheets, считаем что был добавлен админом
                            self.approved_by_admin[telegram_id] = True
                            loaded_from_sheets = True
                        except (ValueError, IndexError):
                            continue
                    # Если Google Sheets доступен, используем его как источник истины (даже если пуст)
                    if loaded_from_sheets or (rows and len(rows) > start_idx):
                        logger.info(f"Сотрудники загружены из Google Sheets: {len(self.employees)} записей")
                        return
                except Exception as e:
                    logger.warning(f"Ошибка загрузки сотрудников из Google Sheets: {e}, используем файлы")
            else:
                logger.debug(f"Есть буферизованные операции для {SHEET_EMPLOYEES}, используем локальные файлы")
        
        # Загружаем из файла
        if not os.path.exists(EMPLOYEES_FILE):
            os.makedirs(DATA_DIR, exist_ok=True)
            return
        
        try:
            with open(EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    
                    parts = line.split(':')
                    # Поддержка старого формата (имя:telegram_id) и нового (имя:имя_телеги:id:никнейм)
                    if len(parts) == 2:
                        # Старый формат: имя:telegram_id
                        manual_name = parts[0].strip()
                        telegram_id = int(parts[1].strip())
                        telegram_name = manual_name
                        username = None
                    elif len(parts) >= 3:
                        # Новый формат: имя_вручную:имя_телеги:telegram_id:никнейм
                        manual_name = parts[0].strip()
                        telegram_name = parts[1].strip() if len(parts) > 1 and parts[1].strip() else manual_name
                        telegram_id = int(parts[2].strip())
                        username = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
                    else:
                        continue
                    
                    # Если уже есть запись с таким ID, пропускаем (будет схлопнуто позже)
                    if telegram_id not in self.employees:
                        self.employees[telegram_id] = (manual_name, telegram_name, username)
                        self.name_to_id[manual_name] = telegram_id
                        # Если загружаем из файла/Google Sheets, считаем что был добавлен админом
                        self.approved_by_admin[telegram_id] = True
        except Exception as e:
            logger.error(f"Ошибка загрузки сотрудников: {e}")
        
        # Если не загрузилось из файла, пробуем загрузить из Google Sheets
        # (но только если USE_GOOGLE_SHEETS_FOR_READS включен и нет буферизованных операций)
        if USE_GOOGLE_SHEETS_FOR_READS and not self.employees and self.sheets_manager and self.sheets_manager.is_available():
            # Проверяем, есть ли буферизованные операции для листа employees
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_EMPLOYEES)
            
            if not has_buffered:
                try:
                    rows = self.sheets_manager.read_all_rows(SHEET_EMPLOYEES)
                    rows = filter_empty_rows(rows)
                    start_idx, _ = get_header_start_idx(rows, ['manual_name', 'Имя вручную'])
                    loaded_from_sheets = False
                    for row in rows[start_idx:]:
                        if len(row) >= 3:
                            try:
                                manual_name = row[0].strip() if row[0] else ""
                                telegram_name = row[1].strip() if row[1] else ""
                                telegram_id = int(row[2].strip())
                                username = row[3].strip() if len(row) >= 4 and row[3] else None
                                
                                self.employees[telegram_id] = (manual_name, telegram_name, username)
                                self.name_to_id[manual_name] = telegram_id
                                self.approved_by_admin[telegram_id] = True
                                loaded_from_sheets = True
                            except (ValueError, IndexError):
                                continue
                    # Если Google Sheets доступен, используем его как источник истины (даже если пуст)
                    if loaded_from_sheets or (rows and len(rows) > start_idx):
                        logger.info(f"Сотрудники загружены из Google Sheets: {len(self.employees)} записей")
                        # Сохраняем в файл для совместимости
                        self._save_employees_to_file_only()
                        # Синхронизируем с PostgreSQL
                        self._sync_employees_to_postgresql()
                        return
                except Exception as e:
                    logger.warning(f"Ошибка загрузки сотрудников из Google Sheets: {e}, используем файлы")
            else:
                logger.debug(f"Есть буферизованные операции для {SHEET_EMPLOYEES}, используем локальные файлы")
    
    def _save_employees_to_file_only(self):
        """Сохранить список сотрудников только в файл (без Google Sheets и PostgreSQL)"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            for telegram_id in sorted(self.employees.keys()):
                manual_name, telegram_name, username = self.employees[telegram_id]
                username_str = username if username else ""
                f.write(f"{manual_name}:{telegram_name}:{telegram_id}:{username_str}\n")
    
    def _sync_employees_to_postgresql(self):
        """Синхронизировать сотрудников с PostgreSQL"""
        if not USE_POSTGRESQL:
            return
        
        try:
            from database_sync import save_employee_to_db_sync
            for telegram_id, (manual_name, telegram_name, username) in self.employees.items():
                approved = self.approved_by_admin.get(telegram_id, False)
                try:
                    save_employee_to_db_sync(telegram_id, manual_name, telegram_name, username, approved)
                except Exception as e:
                    logger.error(f"❌ Ошибка синхронизации сотрудника {telegram_id} с PostgreSQL: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации сотрудников с PostgreSQL: {e}", exc_info=True)
    
    def _sync_employees_to_google_sheets(self):
        """Синхронизировать сотрудников с Google Sheets"""
        if not self.sheets_manager or not self.sheets_manager.is_available():
            return
        
        try:
            rows = [['manual_name', 'telegram_name', 'telegram_id', 'username']]  # Заголовок
            for telegram_id in sorted(self.employees.keys()):
                manual_name, telegram_name, username = self.employees[telegram_id]
                username_str = username if username else ""
                rows.append([manual_name, telegram_name, str(telegram_id), username_str])
            self.sheets_manager.write_rows(SHEET_EMPLOYEES, rows, clear_first=True)
        except Exception as e:
            logger.warning(f"Ошибка синхронизации сотрудников с Google Sheets: {e}")
    
    def _save_employees(self):
        """Сохранить список сотрудников в PostgreSQL и файл"""
        # Сохраняем в файл
        self._save_employees_to_file_only()
        
        # Сохраняем в PostgreSQL (приоритет 1)
        self._sync_employees_to_postgresql()
        #     self._sync_employees_to_google_sheets()
    
    def add_employee(self, name: str, telegram_id: int, telegram_name: Optional[str] = None, username: Optional[str] = None) -> bool:
        """Добавить сотрудника"""
        # Если уже есть запись с таким ID, сохраняем существующие данные
        old_username = None
        old_telegram_name = None
        if telegram_id in self.employees:
            old_manual_name, old_telegram_name, old_username = self.employees[telegram_id]
            # Удаляем старое имя из индекса
            if old_manual_name in self.name_to_id:
                del self.name_to_id[old_manual_name]
        
        # Если имя уже используется другим ID, удаляем старую связь
        if name in self.name_to_id and self.name_to_id[name] != telegram_id:
            old_id = self.name_to_id[name]
            if old_id in self.employees:
                del self.employees[old_id]
        
        # Сохраняем существующие данные, если новые не указаны
        telegram_name = telegram_name or old_telegram_name or name
        username = username if username is not None else old_username
        
        self.employees[telegram_id] = (name, telegram_name, username)
        self.name_to_id[name] = telegram_id
        # Помечаем как одобренного админом
        self.approved_by_admin[telegram_id] = True
        
        # Сохраняем в PostgreSQL
        if USE_POSTGRESQL:
            try:
                from database_sync import save_employee_to_db_sync
                save_employee_to_db_sync(telegram_id, name, telegram_name, username, True)
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения сотрудника {telegram_id} в PostgreSQL: {e}", exc_info=True)
        
        # Сохраняем в Google Sheets и файл
        self._save_employees()
        return True
    
    def get_employee_name(self, telegram_id: int) -> Optional[str]:
        """Получить имя сотрудника по Telegram ID (возвращает имя_вручную)"""
        if telegram_id in self.employees:
            return self.employees[telegram_id][0]
        return None
    
    def get_employee_id(self, name: str) -> Optional[int]:
        """Получить Telegram ID сотрудника по имени"""
        return self.name_to_id.get(name)
    
    def get_telegram_id_by_username(self, username: str) -> Optional[int]:
        """Получить Telegram ID по username (никнейму в Telegram)"""
        username_clean = username.lower().lstrip('@')
        for telegram_id, (_, _, user_username) in self.employees.items():
            if user_username and user_username.lower() == username_clean:
                return telegram_id
        return None
    
    def reload_employees(self):
        """Перезагрузить список сотрудников из Google Sheets или файла"""
        self._load_employees()
    
    def reload_pending_employees(self):
        """Перезагрузить список отложенных сотрудников из Google Sheets или файла"""
        self._load_pending_employees()
    
    def _load_pending_employees(self):
        """Загрузить отложенные записи сотрудников из PostgreSQL (приоритет), Google Sheets или файла"""
        # Очищаем текущие данные
        self.pending_employees = {}
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки при старте
        if USE_POSTGRESQL:
            try:
                from database_sync import load_pending_employees_from_db_sync
                logger.debug("Используем синхронную загрузку отложенных сотрудников из PostgreSQL")
                db_pending = load_pending_employees_from_db_sync()
                logger.debug("load_pending_employees_from_db_sync завершен успешно")
            except Exception as e:
                logger.warning(f"Ошибка загрузки отложенных сотрудников из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
                db_pending = None
            
            if db_pending:
                self.pending_employees = db_pending
                logger.info(f"Отложенные сотрудники загружены из PostgreSQL: {len(self.pending_employees)} записей")
                # Сохраняем в файл для совместимости
                self._save_pending_employees_to_file_only()
                return
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            try:
                # Загружаем администраторов для проверки
                admin_ids = set()
                if USE_POSTGRESQL:
                    try:
                        from database_sync import load_admins_from_db_sync
                        admin_ids = load_admins_from_db_sync()
                    except Exception:
                        pass
                
                # Загружаем сотрудников для проверки username -> telegram_id
                employees_data = {}
                if USE_POSTGRESQL:
                    try:
                        from database_sync import load_employees_from_db_sync
                        db_employees = load_employees_from_db_sync()
                        for telegram_id, (manual_name, telegram_name, username, approved) in db_employees.items():
                            if username:
                                employees_data[username.lower()] = telegram_id
                    except Exception:
                        pass
                
                rows = self.sheets_manager.read_all_rows(SHEET_PENDING_EMPLOYEES)
                rows = filter_empty_rows(rows)
                start_idx, _ = get_header_start_idx(rows, ['username', 'manual_name'])
                
                skipped_admins = []
                for row in rows[start_idx:]:
                    if not row or len(row) < 2:
                        continue
                    try:
                        username = row[0].strip().lower() if row[0] else None
                        manual_name = row[1].strip() if len(row) > 1 and row[1] else None
                        if username and manual_name:
                            # Проверяем, не является ли пользователь администратором
                            telegram_id = employees_data.get(username)
                            if telegram_id and telegram_id in admin_ids:
                                skipped_admins.append(username)
                                logger.warning(f"Пропущен администратор @{username} при загрузке из Google Sheets (не должен быть в pending_employees)")
                                continue
                            self.pending_employees[username] = manual_name
                    except Exception:
                        continue
                
                if skipped_admins:
                    logger.warning(f"Пропущено администраторов при загрузке из Google Sheets: {len(skipped_admins)}")
                
                if self.pending_employees:
                    logger.info(f"Отложенные сотрудники загружены из Google Sheets: {len(self.pending_employees)} записей")
                    # Сохраняем в файл для совместимости
                    self._save_pending_employees_to_file_only()
                    # Синхронизируем с PostgreSQL
                    self._sync_pending_employees_to_postgresql()
                    return
            except Exception as e:
                logger.warning(f"Ошибка загрузки отложенных сотрудников из Google Sheets: {e}")
        
        # ПРИОРИТЕТ 3: Локальные файлы
        if not os.path.exists(PENDING_EMPLOYEES_FILE):
            return
        
        # Загружаем администраторов для проверки
        admin_ids = set()
        if USE_POSTGRESQL:
            try:
                from database_sync import load_admins_from_db_sync
                admin_ids = load_admins_from_db_sync()
            except Exception:
                pass
        
        # Загружаем сотрудников для проверки username -> telegram_id
        employees_data = {}
        if USE_POSTGRESQL:
            try:
                from database_sync import load_employees_from_db_sync
                db_employees = load_employees_from_db_sync()
                for telegram_id, (manual_name, telegram_name, username, approved) in db_employees.items():
                    if username:
                        employees_data[username.lower()] = telegram_id
            except Exception:
                pass
        
        try:
            skipped_admins = []
            with open(PENDING_EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        username = parts[0].strip().lower()
                        manual_name = parts[1].strip()
                        # Проверяем, не является ли пользователь администратором
                        telegram_id = employees_data.get(username)
                        if telegram_id and telegram_id in admin_ids:
                            skipped_admins.append(username)
                            logger.warning(f"Пропущен администратор @{username} при загрузке из файла (не должен быть в pending_employees)")
                            continue
                        self.pending_employees[username] = manual_name
        except Exception as e:
            logger.error(f"Ошибка загрузки отложенных записей: {e}")
        
        if skipped_admins:
            logger.warning(f"Пропущено администраторов при загрузке из файла: {len(skipped_admins)}")
        
        # Синхронизируем с PostgreSQL (если загрузились из файла)
        if self.pending_employees:
            self._sync_pending_employees_to_postgresql()
    
    def _save_pending_employees_to_file_only(self):
        """Сохранить отложенные записи сотрудников только в файл"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(PENDING_EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            for username, manual_name in sorted(self.pending_employees.items()):
                f.write(f"{username}:{manual_name}\n")
    
    def _sync_pending_employees_to_postgresql(self):
        """Синхронизировать отложенных сотрудников с PostgreSQL"""
        if not USE_POSTGRESQL:
            return
        
        try:
            from database_sync import save_pending_employee_to_db_sync
            for username, manual_name in self.pending_employees.items():
                try:
                    save_pending_employee_to_db_sync(username, manual_name)
                except Exception as e:
                    logger.error(f"❌ Ошибка синхронизации отложенного сотрудника {username} с PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации отложенных сотрудников с PostgreSQL: {e}", exc_info=True)
    
    def _sync_pending_employees_to_google_sheets(self):
        """Синхронизировать отложенных сотрудников с Google Sheets"""
        if not self.sheets_manager or not self.sheets_manager.is_available():
            return
        
        try:
            rows = [['username', 'manual_name']]  # Заголовок
            for username, manual_name in sorted(self.pending_employees.items()):
                rows.append([username, manual_name])
            self.sheets_manager.write_rows(SHEET_PENDING_EMPLOYEES, rows, clear_first=True)
        except Exception as e:
            logger.warning(f"Ошибка синхронизации отложенных сотрудников с Google Sheets: {e}")
    
    def _save_pending_employees(self):
        """Сохранить отложенные записи сотрудников в PostgreSQL и файл"""
        # Сохраняем в файл
        self._save_pending_employees_to_file_only()
        
        # Сохраняем в PostgreSQL (приоритет 1)
        self._sync_pending_employees_to_postgresql()
        #     self._sync_pending_employees_to_google_sheets()
    
    def add_pending_employee(self, username: str, manual_name: str) -> tuple[bool, Optional[str]]:
        """
        Добавить отложенную запись сотрудника (когда админ добавляет по username до /start)
        
        Returns:
            (was_existing, old_name): 
            - was_existing: True если запись уже существовала, False если новая
            - old_name: старое имя, если запись существовала, иначе None
        
        Raises:
            ValueError: если пользователь является администратором
        """
        username_lower = username.lower().lstrip('@')
        
        # Проверяем, не является ли пользователь администратором
        if USE_POSTGRESQL:
            try:
                from database_sync import load_admins_from_db_sync, load_employees_from_db_sync
                admin_ids = load_admins_from_db_sync()
                db_employees = load_employees_from_db_sync()
                
                # Ищем telegram_id по username
                telegram_id = None
                for tid, (mname, tname, uname, approved) in db_employees.items():
                    if uname and uname.lower() == username_lower:
                        telegram_id = tid
                        break
                
                if telegram_id and telegram_id in admin_ids:
                    logger.warning(f"Попытка добавить администратора @{username_lower} в pending_employees - отклонено")
                    raise ValueError(f"Пользователь @{username_lower} является администратором и не может быть добавлен в pending_employees")
            except ValueError:
                raise  # Пробрасываем ValueError дальше
            except Exception as e:
                logger.warning(f"Ошибка проверки администратора при добавлении в pending_employees: {e}")
                # Продолжаем, если проверка не удалась
        
        was_existing = username_lower in self.pending_employees
        old_name = self.pending_employees.get(username_lower) if was_existing else None
        self.pending_employees[username_lower] = manual_name
        
        # Сохраняем в PostgreSQL
        if USE_POSTGRESQL:
            try:
                from database_sync import save_pending_employee_to_db_sync
                save_pending_employee_to_db_sync(username_lower, manual_name)
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения отложенного сотрудника {username_lower} в PostgreSQL: {e}", exc_info=True)
        
        # Сохраняем в Google Sheets и файл
        self._save_pending_employees()
        return (was_existing, old_name)
    
    def get_pending_employee(self, username: str) -> Optional[str]:
        """Получить имя вручную из отложенной записи по username"""
        username_lower = username.lower().lstrip('@')
        return self.pending_employees.get(username_lower)
    
    def remove_pending_employee(self, username: str):
        """Удалить отложенную запись после успешной регистрации"""
        username_lower = username.lower().lstrip('@')
        if username_lower in self.pending_employees:
            del self.pending_employees[username_lower]
            
            # Удаляем из PostgreSQL
            if USE_POSTGRESQL:
                try:
                    from database_sync import remove_pending_employee_from_db_sync
                    remove_pending_employee_from_db_sync(username_lower)
                except Exception as e:
                    logger.error(f"❌ Ошибка удаления отложенного сотрудника {username_lower} из PostgreSQL: {e}", exc_info=True)
            
            # Сохраняем в Google Sheets и файл
            self._save_pending_employees()
    
    def register_user(self, telegram_id: int, telegram_name: str, username: Optional[str] = None) -> tuple[bool, bool]:
        """
        Зарегистрировать пользователя (если его еще нет)
        Возвращает (was_new, was_added_by_admin):
        - was_new: True если пользователь был зарегистрирован сейчас, False если уже был
        - was_added_by_admin: True если пользователь был добавлен админом (через pending или уже был в системе)
        """
        if telegram_id in self.employees:
            # Обновляем данные, если они изменились
            manual_name, old_telegram_name, old_username = self.employees[telegram_id]
            # Обновляем имя из Telegram и username, но сохраняем имя вручную
            updated = False
            if telegram_name != old_telegram_name or (username and username != old_username):
                self.employees[telegram_id] = (manual_name, telegram_name, username or old_username)
                
                # Сохраняем в PostgreSQL
                if USE_POSTGRESQL:
                    approved = self.approved_by_admin.get(telegram_id, True)
                    try:
                        from database_sync import save_employee_to_db_sync
                        save_employee_to_db_sync(telegram_id, manual_name, telegram_name, username or old_username, approved)
                    except Exception as e:
                        logger.error(f"❌ Ошибка обновления сотрудника {telegram_id} в PostgreSQL: {e}", exc_info=True)
                
                # Сохраняем в Google Sheets и файл
                self._save_employees()
                updated = True
            # Если пользователь уже был в системе, считаем что был добавлен админом
            was_added = self.approved_by_admin.get(telegram_id, True)
            return (False, was_added)  # Уже зарегистрирован
        
        # Проверяем, есть ли отложенная запись для этого username
        manual_name = telegram_name  # По умолчанию используем имя из Telegram
        was_added_by_admin = False
        if username:
            pending_name = self.get_pending_employee(username)
            if pending_name:
                manual_name = pending_name
                was_added_by_admin = True
                # Удаляем отложенную запись, так как пользователь зарегистрирован
                self.remove_pending_employee(username)
        
        # Используем имя вручную (из отложенной записи или из Telegram)
        self.employees[telegram_id] = (manual_name, telegram_name, username)
        self.name_to_id[manual_name] = telegram_id
        # Сохраняем флаг одобрения админом
        self.approved_by_admin[telegram_id] = was_added_by_admin
        
        # Сохраняем в PostgreSQL
        if USE_POSTGRESQL:
            try:
                from database_sync import save_employee_to_db_sync
                save_employee_to_db_sync(telegram_id, manual_name, telegram_name, username, was_added_by_admin)
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения сотрудника {telegram_id} в PostgreSQL: {e}", exc_info=True)
        
        # Сохраняем в Google Sheets и файл
        self._save_employees()
        return (True, was_added_by_admin)
    
    def get_all_employees(self) -> Dict[str, int]:
        """Получить всех сотрудников (имя -> telegram_id)"""
        return self.name_to_id.copy()
    
    def get_all_telegram_ids(self) -> List[int]:
        """Получить все Telegram ID"""
        return list(self.employees.keys())
    
    def is_registered(self, telegram_id: int) -> bool:
        """Проверить, зарегистрирован ли пользователь"""
        return telegram_id in self.employees
    
    def was_added_by_admin(self, telegram_id: int) -> bool:
        """
        Проверить, был ли пользователь добавлен администратором.
        Если пользователь сам себя зарегистрировал через /start (без pending записи),
        то manual_name будет равен telegram_name, и это считается "не добавлен админом".
        Но это не надежно, поэтому лучше проверять через другой механизм.
        
        Более надежный способ: если пользователь был в pending или был добавлен до первого /start.
        Для простоты: если manual_name != telegram_name, значит был добавлен админом.
        Или если был в pending записи (но она уже удалена после регистрации).
        
        Упрощенная логика: если пользователь зарегистрирован, считаем что он был добавлен админом,
        если только он не сам себя зарегистрировал. Но как это определить?
        
        Лучше добавить флаг при регистрации или проверять по другому критерию.
        Пока используем упрощенную проверку: если есть в employees, значит был добавлен.
        """
        if telegram_id not in self.employees:
            return False
        
        # Если пользователь в системе, считаем что он был добавлен админом
        # (так как мы не сохраняем информацию о том, кто его добавил)
        # В реальности, если пользователь сам себя зарегистрировал, manual_name == telegram_name
        # Но это не надежно, так как админ мог добавить с таким же именем
        
        # Для более точной проверки можно было бы добавить флаг в данные сотрудника
        # Но пока используем упрощенную логику: если в системе, значит добавлен
        return True
    
    def get_employee_data(self, telegram_id: int) -> Optional[Tuple[str, str, Optional[str]]]:
        """Получить данные сотрудника по Telegram ID (имя_вручную, имя_телеги, никнейм)"""
        return self.employees.get(telegram_id)
    
    def format_employee_name(self, employee_name: str) -> str:
        """Форматировать имя сотрудника для отображения: имя(@никнейм)"""
        telegram_id = self.name_to_id.get(employee_name)
        if telegram_id and telegram_id in self.employees:
            _, _, username = self.employees[telegram_id]
            if username:
                return f"{employee_name}(@{username})"
        return employee_name
    
    def format_employee_name_by_id(self, telegram_id: int) -> str:
        """Форматировать имя сотрудника по ID для отображения: имя(@никнейм)"""
        if telegram_id in self.employees:
            manual_name, _, username = self.employees[telegram_id]
            if username:
                return f"{manual_name}(@{username})"
            return manual_name
        return str(telegram_id)
    
    def merge_duplicates(self):
        """Схлопнуть дубликаты по telegram_id (оставить последнюю запись для каждого ID)"""
        # Используем текущие данные из памяти (которые уже загружены из Google Sheets или файла)
        # Схлопываем дубликаты - оставляем последнюю запись для каждого telegram_id
        # (в словаре уже хранится последняя запись для каждого ID, так что просто сохраняем)
        
        # Обновляем индекс name_to_id на основе текущих данных
        self.name_to_id = {}
        for telegram_id, (manual_name, _, _) in self.employees.items():
            self.name_to_id[manual_name] = telegram_id
        
        # Сохраняем схлопнутые данные (это обновит и Google Sheets, и файл)
        self._save_employees()
