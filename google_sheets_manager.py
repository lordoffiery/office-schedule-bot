"""
Модуль для работы с Google Sheets в качестве хранилища данных
"""
import os
import json
from typing import Dict, List, Optional, Any
import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsManager:
    """Класс для управления данными через Google Sheets"""
    
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self._init_client()
    
    def _init_client(self):
        """Инициализировать клиент Google Sheets"""
        # Получаем credentials из переменной окружения или файла
        credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
        spreadsheet_id = os.getenv('GOOGLE_SHEETS_ID')
        credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')
        
        # Если нет переменных окружения, пробуем использовать файл
        if not credentials_json:
            if os.path.exists(credentials_file):
                try:
                    with open(credentials_file, 'r') as f:
                        credentials_dict = json.load(f)
                    credentials_json = json.dumps(credentials_dict)
                except Exception as e:
                    print(f"⚠️ Не удалось прочитать файл credentials: {e}")
        
        if not credentials_json or not spreadsheet_id:
            print("⚠️ Google Sheets не настроен. Используются локальные файлы.")
            return None
        
        try:
            # Парсим JSON credentials
            if isinstance(credentials_json, str):
                credentials_dict = json.loads(credentials_json)
            else:
                credentials_dict = credentials_json
            
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_info(credentials_dict, scopes=scopes)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(spreadsheet_id)
            print(f"✅ Google Sheets подключен (ID: {spreadsheet_id})")
        except Exception as e:
            print(f"❌ Ошибка подключения к Google Sheets: {e}")
            self.client = None
            self.spreadsheet = None
    
    def is_available(self) -> bool:
        """Проверить, доступен ли Google Sheets"""
        return self.client is not None and self.spreadsheet is not None
    
    def get_worksheet(self, name: str, create_if_missing: bool = True):
        """Получить лист по имени, создать если не существует"""
        if not self.is_available():
            return None
        
        try:
            return self.spreadsheet.worksheet(name)
        except gspread.exceptions.WorksheetNotFound:
            if create_if_missing:
                return self.spreadsheet.add_worksheet(title=name, rows=1000, cols=20)
            return None
    
    def read_all_rows(self, worksheet_name: str) -> List[List[str]]:
        """Прочитать все строки из листа"""
        if not self.is_available():
            return []
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return []
        
        try:
            return worksheet.get_all_values()
        except Exception as e:
            print(f"Ошибка чтения из {worksheet_name}: {e}")
            return []
    
    def write_rows(self, worksheet_name: str, rows: List[List[str]], clear_first: bool = True):
        """Записать строки в лист"""
        if not self.is_available():
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            if clear_first:
                worksheet.clear()
            if rows:
                worksheet.update(rows, value_input_option='RAW')
            return True
        except Exception as e:
            print(f"Ошибка записи в {worksheet_name}: {e}")
            return False
    
    def append_row(self, worksheet_name: str, row: List[str]):
        """Добавить строку в конец листа"""
        if not self.is_available():
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            worksheet.append_row(row, value_input_option='RAW')
            return True
        except Exception as e:
            print(f"Ошибка добавления строки в {worksheet_name}: {e}")
            return False
    
    def find_and_update_row(self, worksheet_name: str, search_col: int, search_value: str, new_row: List[str]):
        """Найти строку по значению в колонке и обновить её"""
        if not self.is_available():
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            all_rows = worksheet.get_all_values()
            for i, row in enumerate(all_rows, start=1):
                if len(row) > search_col and str(row[search_col]) == str(search_value):
                    # Обновляем строку
                    worksheet.update(f'A{i}', [new_row], value_input_option='RAW')
                    return True
            return False
        except Exception as e:
            print(f"Ошибка обновления строки в {worksheet_name}: {e}")
            return False
    
    def find_and_delete_row(self, worksheet_name: str, search_col: int, search_value: str):
        """Найти строку по значению в колонке и удалить её"""
        if not self.is_available():
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            all_rows = worksheet.get_all_values()
            for i, row in enumerate(all_rows, start=1):
                if len(row) > search_col and str(row[search_col]) == str(search_value):
                    worksheet.delete_rows(i)
                    return True
            return False
        except Exception as e:
            print(f"Ошибка удаления строки в {worksheet_name}: {e}")
            return False
    
    def get_cell_value(self, worksheet_name: str, cell: str) -> Optional[str]:
        """Получить значение ячейки"""
        if not self.is_available():
            return None
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return None
        
        try:
            return worksheet.acell(cell).value
        except Exception as e:
            print(f"Ошибка чтения ячейки {cell} из {worksheet_name}: {e}")
            return None
    
    def set_cell_value(self, worksheet_name: str, cell: str, value: str):
        """Установить значение ячейки"""
        if not self.is_available():
            return False
        
        worksheet = self.get_worksheet(worksheet_name)
        if not worksheet:
            return False
        
        try:
            worksheet.update(cell, value, value_input_option='RAW')
            return True
        except Exception as e:
            print(f"Ошибка записи ячейки {cell} в {worksheet_name}: {e}")
            return False

