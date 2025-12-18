"""
Скрипт для проверки расписания по умолчанию в PostgreSQL и Google Sheets
"""
import os
import json
import sys

os.environ['BOT_TOKEN'] = os.getenv('BOT_TOKEN', 'check_token')
os.environ['USE_GOOGLE_SHEETS'] = 'true'

from config import USE_GOOGLE_SHEETS, SHEET_DEFAULT_SCHEDULE
from database_sync import load_default_schedule_from_db_sync
from utils import get_header_start_idx, filter_empty_rows

if USE_GOOGLE_SHEETS:
    from google_sheets_manager import GoogleSheetsManager
else:
    print("❌ USE_GOOGLE_SHEETS отключен")
    sys.exit(1)

# Загружаем из Google Sheets
sheets_manager = GoogleSheetsManager()
if not sheets_manager.is_available():
    print("❌ Google Sheets недоступен")
    sys.exit(1)

rows = sheets_manager.read_all_rows(SHEET_DEFAULT_SCHEDULE)
if not rows:
    print("⚠️ Google Sheets: расписание по умолчанию не найдено")
    sheets_schedule = {}
else:
    start_idx, _ = get_header_start_idx(rows, ['day_name', 'places_json'])
    sheets_schedule = {}
    for row in rows[start_idx:]:
        if not row or len(row) < 2:
            continue
        day_name = row[0].strip()
        places_json = row[1].strip() if len(row) > 1 else '{}'
        try:
            places_dict = json.loads(places_json)
            sheets_schedule[day_name] = places_dict
        except json.JSONDecodeError:
            continue

# Загружаем из PostgreSQL
db_schedule = load_default_schedule_from_db_sync()

print("=" * 80)
print("📋 Расписание по умолчанию")
print("=" * 80)

print(f"\n📊 Google Sheets: {len(sheets_schedule)} дней")
print(f"📊 PostgreSQL: {len(db_schedule)} дней")

all_days = set(sheets_schedule.keys()) | set(db_schedule.keys())

for day in sorted(all_days):
    print(f"\n{'=' * 80}")
    print(f"📅 {day}")
    print(f"{'=' * 80}")
    
    sheets_data = sheets_schedule.get(day, {})
    db_data = db_schedule.get(day, {})
    
    print(f"\n📄 Google Sheets ({len(sheets_data)} мест):")
    for place in sorted(sheets_data.keys()):
        employee = sheets_data[place]
        print(f"   {place}: {employee if employee else '(пусто)'}")
    
    print(f"\n💾 PostgreSQL ({len(db_data)} мест):")
    for place in sorted(db_data.keys()):
        employee = db_data[place]
        print(f"   {place}: {employee if employee else '(пусто)'}")
    
    if sheets_data != db_data:
        print(f"\n⚠️ РАЗЛИЧИЯ:")
        all_places = set(sheets_data.keys()) | set(db_data.keys())
        for place in sorted(all_places):
            sheets_emp = sheets_data.get(place, '')
            db_emp = db_data.get(place, '')
            if sheets_emp != db_emp:
                print(f"   {place}:")
                print(f"      Google Sheets: {sheets_emp if sheets_emp else '(пусто)'}")
                print(f"      PostgreSQL:    {db_emp if db_emp else '(пусто)'}")
    else:
        print(f"\n✅ Идентично")

print("\n" + "=" * 80)

