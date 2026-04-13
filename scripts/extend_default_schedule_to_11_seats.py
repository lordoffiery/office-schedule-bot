#!/usr/bin/env python3
"""
Одноразово дополняет default_schedule в PostgreSQL ключами 1.9, 1.10, 1.11 (пустые строки),
если их ещё нет. Не затирает существующие места.

Использование (важно: тот же Python, куда ставили pip-пакеты — не смешивать Homebrew и conda):
  conda activate env_3_13   # или ваш venv
  export DATABASE_URL='postgresql://...'
  python scripts/extend_default_schedule_to_11_seats.py
  python scripts/extend_default_schedule_to_11_seats.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Callable

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

_pg_connect: Callable[[str], Any] | None = None
_pg_dict_cursor: Any = None

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor

    def _connect_psycopg2(url: str):
        return psycopg2.connect(url)

    _pg_connect = _connect_psycopg2
    _pg_dict_cursor = RealDictCursor
except ImportError:
    try:
        import psycopg
        from psycopg.rows import dict_row

        def _connect_psycopg3(url: str):
            return psycopg.connect(url, row_factory=dict_row)

        _pg_connect = _connect_psycopg3
        _pg_dict_cursor = None
    except ImportError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="только показать изменения")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PUBLIC_URL")
    if not database_url:
        print("Задайте DATABASE_URL или DATABASE_PUBLIC_URL", file=sys.stderr)
        return 1

    if _pg_connect is None:
        print(
            f"""Нет драйвера PostgreSQL в этом интерпретаторе Python:
  {sys.executable}

Частая причина: пакет ставили в conda/venv, а скрипт запускают через /opt/homebrew/bin/python3
(или наоборот). Нужен один и тот же интерпретатор для pip и для запуска.

Проверка:  which python   и   python -c "import psycopg2"

Установка (в том же окружении, откуда запускаете скрипт):
  pip install psycopg2-binary
  или: pip install 'psycopg[binary]'
Из каталога бота: pip install -r requirements.txt""",
            file=sys.stderr,
        )
        return 1

    conn = _pg_connect(database_url)
    try:
        cur_kw: dict[str, Any] = {}
        if _pg_dict_cursor is not None:
            cur_kw["cursor_factory"] = _pg_dict_cursor
        with conn.cursor(**cur_kw) as cur:
            cur.execute("SELECT day_name, places_json FROM default_schedule ORDER BY day_name")
            rows = cur.fetchall()

        if not rows:
            print("Таблица default_schedule пуста — нечего обновлять.")
            return 0

        extra = {"1.9": "", "1.10": "", "1.11": ""}
        updates: list[tuple[str, str]] = []

        for row in rows:
            day_name = row["day_name"]
            try:
                places = json.loads(row["places_json"])
            except json.JSONDecodeError as e:
                print(f"Пропуск {day_name}: невалидный JSON: {e}", file=sys.stderr)
                continue
            if not isinstance(places, dict):
                print(f"Пропуск {day_name}: places_json не объект", file=sys.stderr)
                continue

            added_keys: list[str] = []
            for k, v in extra.items():
                if k not in places:
                    places[k] = v
                    added_keys.append(k)

            if added_keys:
                new_json = json.dumps(places, ensure_ascii=False)
                updates.append((day_name, new_json))
                print(f"  {day_name}: добавлены ключи {', '.join(added_keys)}")
            else:
                print(f"  {day_name}: уже есть 1.9–1.11, пропуск")

        if args.dry_run:
            print(f"[dry-run] было бы обновлено строк: {len(updates)}")
            return 0

        with conn.cursor() as cur:
            for day_name, places_json in updates:
                cur.execute(
                    """
                    UPDATE default_schedule
                    SET places_json = %s, updated_at = NOW()
                    WHERE day_name = %s
                    """,
                    (places_json, day_name),
                )
        conn.commit()
        print(f"Готово: обновлено {len(updates)} дней.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
