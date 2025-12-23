"""
Управление администраторами
"""
import os
import logging
import asyncio
from typing import List, Set
from config import ADMINS_FILE, DATA_DIR, ADMIN_IDS, USE_GOOGLE_SHEETS, USE_GOOGLE_SHEETS_FOR_WRITES, USE_GOOGLE_SHEETS_FOR_READS, SHEET_ADMINS, USE_POSTGRESQL
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
        from database import load_admins_from_db, save_admins_to_db, add_admin_to_db, remove_admin_from_db
    except ImportError:
        load_admins_from_db = None
        save_admins_to_db = None
        add_admin_to_db = None
        remove_admin_from_db = None
else:
    load_admins_from_db = None
    save_admins_to_db = None
    add_admin_to_db = None
    remove_admin_from_db = None


def _get_pool():
    """Получить пул подключений PostgreSQL (динамический импорт)"""
    if not USE_POSTGRESQL:
        return None
    try:
        from database import _pool
        return _pool
    except ImportError:
        return None


class AdminManager:
    """Класс для управления списком администраторов"""
    
    def __init__(self):
        self.admins: Set[int] = set()  # Начинаем с пустого множества, загрузим из Google Sheets/файла
        
        # Инициализируем Google Sheets Manager если нужно
        self.sheets_manager = None
        if USE_GOOGLE_SHEETS and GoogleSheetsManager:
            try:
                self.sheets_manager = GoogleSheetsManager()
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Google Sheets для админов: {e}")
        
        self._load_admins()
    
    def reload_admins(self):
        """Перезагрузить список администраторов из Google Sheets или файла"""
        self._load_admins()
    
    def _load_admins(self):
        """Загрузить список администраторов из PostgreSQL (приоритет), Google Sheets или файла"""
        # Очищаем текущие данные перед загрузкой
        self.admins = set()
        
        # ПРИОРИТЕТ 1: PostgreSQL (если доступен)
        # Используем синхронные функции для загрузки при старте
        if USE_POSTGRESQL:
            try:
                from database_sync import load_admins_from_db_sync
                logger.debug("Используем синхронную загрузку администраторов из PostgreSQL")
                db_admins = load_admins_from_db_sync()
                logger.debug("load_admins_from_db_sync завершен успешно")
            except Exception as e:
                logger.warning(f"Ошибка загрузки администраторов из PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
                db_admins = None
            
            if db_admins:
                self.admins = db_admins
                logger.info(f"Администраторы загружены из PostgreSQL: {len(self.admins)} записей")
                # Сохраняем в файл для совместимости
                self._save_admins_to_file_only()
                # Синхронизируем с Google Sheets (если доступен)
                self._sync_to_google_sheets()
                return
        
        # ПРИОРИТЕТ 2: Google Sheets (только если USE_GOOGLE_SHEETS_FOR_READS включен и PostgreSQL недоступен)
        if USE_GOOGLE_SHEETS_FOR_READS and self.sheets_manager and self.sheets_manager.is_available():
            # Проверяем, есть ли буферизованные операции для листа admins
            has_buffered = self.sheets_manager.has_buffered_operations_for_sheet(SHEET_ADMINS)
            
            if not has_buffered:
                try:
                    rows = self.sheets_manager.read_all_rows(SHEET_ADMINS)
                    rows = filter_empty_rows(rows)
                    start_idx, _ = get_header_start_idx(rows, ['admin_id', 'telegram_id', 'ID'])
                    
                    # Если в Google Sheets есть данные, используем их как источник истины
                    if rows and len(rows) > start_idx:
                        sheets_admins = set()
                        for row in rows[start_idx:]:
                            if row and row[0]:
                                try:
                                    admin_id = int(row[0].strip())
                                    sheets_admins.add(admin_id)
                                except ValueError:
                                    continue
                        
                        if sheets_admins:
                            self.admins = sheets_admins
                            logger.info(f"Администраторы загружены из Google Sheets: {len(self.admins)} записей")
                            # Сохраняем в файл и PostgreSQL для синхронизации
                            self._save_admins_to_file_only()
                            # Синхронизируем с PostgreSQL (если доступен)
                            self._sync_to_postgresql()
                            return
                except Exception as e:
                    logger.warning(f"Ошибка загрузки администраторов из Google Sheets: {e}")
            else:
                logger.debug(f"Есть буферизованные операции для {SHEET_ADMINS}, используем данные из файла")
        
        # ПРИОРИТЕТ 3: Локальные файлы
        if os.path.exists(ADMINS_FILE):
            try:
                with open(ADMINS_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            admin_id = int(line)
                            self.admins.add(admin_id)
                        except ValueError:
                            continue
                if self.admins:
                    logger.info(f"Администраторы загружены из файла: {len(self.admins)} записей")
            except Exception as e:
                logger.error(f"Ошибка загрузки администраторов из файла: {e}")
        
        # Если ничего не загрузилось, используем админов из config как fallback
        if not self.admins:
            self.admins = set(ADMIN_IDS)
            logger.info(f"Используются администраторы из config: {len(self.admins)} записей")
        
        # Сохраняем в файл для совместимости
        self._save_admins_to_file_only()
        
        # Синхронизируем с PostgreSQL (если загрузились из файла/Google Sheets, но не из PostgreSQL)
        # Это нужно для первичной синхронизации данных
        pool = _get_pool()
        if USE_POSTGRESQL and pool:
            self._sync_to_postgresql()
    
    def _save_admins_to_file_only(self):
        """Сохранить список администраторов только в файл (без Google Sheets и PostgreSQL)"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(ADMINS_FILE, 'w', encoding='utf-8') as f:
            for admin_id in sorted(self.admins):
                f.write(f"{admin_id}\n")
    
    def _sync_to_postgresql(self):
        """Синхронизировать администраторов с PostgreSQL"""
        if not USE_POSTGRESQL:
            return
        
        try:
            from database_sync import save_admins_to_db_sync
            save_admins_to_db_sync(self.admins)
            logger.debug(f"Администраторы синхронизированы с PostgreSQL: {len(self.admins)} записей")
        except Exception as e:
            logger.warning(f"Ошибка синхронизации с PostgreSQL: {type(e).__name__}: {e}", exc_info=True)
    
    def _sync_to_google_sheets(self):
        """Синхронизировать администраторов с Google Sheets"""
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = [['admin_id']]  # Заголовок
                for admin_id in sorted(self.admins):
                    rows.append([str(admin_id)])
                self.sheets_manager.write_rows(SHEET_ADMINS, rows, clear_first=True)
            except Exception as e:
                logger.warning(f"Ошибка синхронизации с Google Sheets: {e}")
    
    def _save_admins(self):
        """Сохранить список администраторов в PostgreSQL, Google Sheets и файл"""
        # Сохраняем в файл
        self._save_admins_to_file_only()
        
        # Сохраняем в PostgreSQL (приоритет 1)
        self._sync_to_postgresql()
        #     self._sync_to_google_sheets()
    
    def add_admin(self, telegram_id: int) -> bool:
        """Добавить администратора"""
        if telegram_id in self.admins:
            return False  # Уже админ
        
        # Сохраняем в PostgreSQL ПЕРВЫМ (приоритет 1)
        if USE_POSTGRESQL:
            try:
                from database_sync import add_admin_to_db_sync
                result = add_admin_to_db_sync(telegram_id)
                if result:
                    logger.info(f"✅ Админ {telegram_id} добавлен в PostgreSQL")
                else:
                    logger.warning(f"⚠️ Админ {telegram_id} не добавлен в PostgreSQL (вернуло False)")
                    return False
            except Exception as e:
                logger.error(f"❌ Ошибка добавления администратора {telegram_id} в PostgreSQL: {e}", exc_info=True)
                # Не обновляем память, если не удалось сохранить в PostgreSQL
                return False
        
        # Обновляем память только после успешного сохранения в PostgreSQL
        self.admins.add(telegram_id)
        
        # Сохраняем в Google Sheets и файл
        self._save_admins()
        return True
    
    def remove_admin(self, telegram_id: int) -> bool:
        """Удалить администратора"""
        if telegram_id not in self.admins:
            return False  # Не является админом
        
        # Удаляем из PostgreSQL ПЕРВЫМ (приоритет 1)
        if USE_POSTGRESQL:
            try:
                from database_sync import remove_admin_from_db_sync
                result = remove_admin_from_db_sync(telegram_id)
                if result:
                    logger.info(f"✅ Админ {telegram_id} удален из PostgreSQL")
                else:
                    logger.warning(f"⚠️ Админ {telegram_id} не удален из PostgreSQL (вернуло False)")
                    return False
            except Exception as e:
                logger.error(f"❌ Ошибка удаления администратора {telegram_id} из PostgreSQL: {e}", exc_info=True)
                # Не обновляем память, если не удалось удалить из PostgreSQL
                return False
        
        # Обновляем память только после успешного удаления из PostgreSQL
        self.admins.remove(telegram_id)
        
        # Сохраняем в Google Sheets и файл
        self._save_admins()
        return True
    
    def is_admin(self, telegram_id: int) -> bool:
        """Проверить, является ли пользователь администратором"""
        return telegram_id in self.admins
    
    def get_all_admins(self) -> List[int]:
        """Получить всех администраторов"""
        return sorted(list(self.admins))

