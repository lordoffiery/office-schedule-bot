"""
Управление администраторами
"""
import os
from typing import List, Set
from config import ADMINS_FILE, DATA_DIR, ADMIN_IDS, USE_GOOGLE_SHEETS, SHEET_ADMINS

# Импортируем Google Sheets Manager только если нужно
if USE_GOOGLE_SHEETS:
    try:
        from google_sheets_manager import GoogleSheetsManager
    except ImportError:
        GoogleSheetsManager = None
else:
    GoogleSheetsManager = None


class AdminManager:
    """Класс для управления списком администраторов"""
    
    def __init__(self):
        self.admins: Set[int] = set(ADMIN_IDS)  # Начинаем с админов из config
        
        # Инициализируем Google Sheets Manager если нужно
        self.sheets_manager = None
        if USE_GOOGLE_SHEETS and GoogleSheetsManager:
            try:
                self.sheets_manager = GoogleSheetsManager()
            except Exception as e:
                print(f"⚠️ Не удалось инициализировать Google Sheets для админов: {e}")
        
        self._load_admins()
    
    def _load_admins(self):
        """Загрузить список администраторов из файла или Google Sheets"""
        # Пробуем загрузить из Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = self.sheets_manager.read_all_rows(SHEET_ADMINS)
                # Пропускаем заголовок, если есть
                start_idx = 1 if rows and len(rows) > 0 and rows[0][0] in ['admin_id', 'telegram_id', 'ID'] else 0
                for row in rows[start_idx:]:
                    if row and row[0]:
                        try:
                            admin_id = int(row[0].strip())
                            self.admins.add(admin_id)
                        except ValueError:
                            continue
                # Если загрузили из Google Sheets, сохраняем в файл для совместимости
                if self.admins:
                    self._save_admins()
                    return
            except Exception as e:
                print(f"Ошибка загрузки администраторов из Google Sheets: {e}, используем файлы")
        
        # Загружаем из файла
        if not os.path.exists(ADMINS_FILE):
            os.makedirs(DATA_DIR, exist_ok=True)
            # Сохраняем начальных админов в файл
            self._save_admins()
            return
        
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
        except Exception as e:
            print(f"Ошибка загрузки администраторов: {e}")
    
    def _save_admins(self):
        """Сохранить список администраторов в файл или Google Sheets"""
        # Пробуем сохранить в Google Sheets
        if self.sheets_manager and self.sheets_manager.is_available():
            try:
                rows = [['admin_id']]  # Заголовок
                for admin_id in sorted(self.admins):
                    rows.append([str(admin_id)])
                self.sheets_manager.write_rows(SHEET_ADMINS, rows, clear_first=True)
            except Exception as e:
                print(f"Ошибка сохранения администраторов в Google Sheets: {e}, используем файлы")
        
        # Сохраняем в файл
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(ADMINS_FILE, 'w', encoding='utf-8') as f:
            for admin_id in sorted(self.admins):
                f.write(f"{admin_id}\n")
    
    def add_admin(self, telegram_id: int) -> bool:
        """Добавить администратора"""
        if telegram_id in self.admins:
            return False  # Уже админ
        self.admins.add(telegram_id)
        self._save_admins()
        return True
    
    def remove_admin(self, telegram_id: int) -> bool:
        """Удалить администратора"""
        if telegram_id not in self.admins:
            return False  # Не является админом
        self.admins.remove(telegram_id)
        self._save_admins()
        return True
    
    def is_admin(self, telegram_id: int) -> bool:
        """Проверить, является ли пользователь администратором"""
        return telegram_id in self.admins
    
    def get_all_admins(self) -> List[int]:
        """Получить всех администраторов"""
        return sorted(list(self.admins))

