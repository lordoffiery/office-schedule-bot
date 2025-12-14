"""
Управление сотрудниками
"""
import os
from typing import Dict, Optional, List
from config import EMPLOYEES_FILE, DATA_DIR


class EmployeeManager:
    """Класс для управления списком сотрудников"""
    
    def __init__(self):
        self.employees: Dict[str, int] = {}  # имя -> telegram_id
        self.telegram_to_name: Dict[int, str] = {}  # telegram_id -> имя
        self._load_employees()
    
    def _load_employees(self):
        """Загрузить список сотрудников из файла"""
        if not os.path.exists(EMPLOYEES_FILE):
            os.makedirs(DATA_DIR, exist_ok=True)
            return
        
        try:
            with open(EMPLOYEES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or ':' not in line:
                        continue
                    name, telegram_id = line.split(':', 1)
                    telegram_id = int(telegram_id.strip())
                    self.employees[name.strip()] = telegram_id
                    self.telegram_to_name[telegram_id] = name.strip()
        except Exception as e:
            print(f"Ошибка загрузки сотрудников: {e}")
    
    def _save_employees(self):
        """Сохранить список сотрудников в файл"""
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(EMPLOYEES_FILE, 'w', encoding='utf-8') as f:
            for name, telegram_id in self.employees.items():
                f.write(f"{name}:{telegram_id}\n")
    
    def add_employee(self, name: str, telegram_id: int) -> bool:
        """Добавить сотрудника"""
        if name in self.employees:
            return False
        self.employees[name] = telegram_id
        self.telegram_to_name[telegram_id] = name
        self._save_employees()
        return True
    
    def get_employee_name(self, telegram_id: int) -> Optional[str]:
        """Получить имя сотрудника по Telegram ID"""
        return self.telegram_to_name.get(telegram_id)
    
    def get_employee_id(self, name: str) -> Optional[int]:
        """Получить Telegram ID сотрудника по имени"""
        return self.employees.get(name)
    
    def register_user(self, telegram_id: int, name: str) -> bool:
        """Зарегистрировать пользователя (если его еще нет)"""
        if telegram_id in self.telegram_to_name:
            return False  # Уже зарегистрирован
        self.employees[name] = telegram_id
        self.telegram_to_name[telegram_id] = name
        self._save_employees()
        return True
    
    def get_all_employees(self) -> Dict[str, int]:
        """Получить всех сотрудников"""
        return self.employees.copy()
    
    def get_all_telegram_ids(self) -> List[int]:
        """Получить все Telegram ID"""
        return list(self.telegram_to_name.keys())
    
    def is_registered(self, telegram_id: int) -> bool:
        """Проверить, зарегистрирован ли пользователь"""
        return telegram_id in self.telegram_to_name

