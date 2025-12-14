"""
Управление администраторами
"""
import os
from typing import List, Set
from config import ADMINS_FILE, DATA_DIR, ADMIN_IDS


class AdminManager:
    """Класс для управления списком администраторов"""
    
    def __init__(self):
        self.admins: Set[int] = set(ADMIN_IDS)  # Начинаем с админов из config
        self._load_admins()
    
    def _load_admins(self):
        """Загрузить список администраторов из файла"""
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
        """Сохранить список администраторов в файл"""
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

