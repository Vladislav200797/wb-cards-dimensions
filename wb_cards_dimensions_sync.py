import os
import math
import time
import requests
from supabase import create_client, Client


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
WB_API_TOKEN = os.environ["WB_API_TOKEN_CONTENT"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

WB_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"


def iter_wb_cards(limit: int = 100):
    """
    Генератор, который проходится по всем карточкам WB через пагинацию.
    """
    headers = {
        "Authorization": WB_API_TOKEN,
        "Content-Type": "application/json",
    }

    cursor = {"limit": limit}
    filter_ = {
        "withPhoto": -1,   # неважно, просто тупо всё
        # можно добавить textSearch / brand / objectIDs, если захочешь фильтрацию
    }

    while True:
        payload = {
            "settings": {
                "cursor": cursor,
                "filter": filter_,
                "sort": {
                    "ascending": False
                },
            }
        }

        resp = requests.post(WB_URL, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        cards = data.get("cards", []) or []
        if not cards:
            break

        for card in cards:
            yield card

        cur = data.get("cursor") or {}
        total = cur.get("total", 0)
        limit = cur.get("limit", cursor.get("limit", limit))

        # если total < limit — всё выгрузили
        if total < limit:
            break

        # готовим курсор для следующего запроса
        cursor = {
            "updatedAt": cur.get("updatedAt"),
            "nmID": cur.get("nmID"),
            "limit": limit,
        }


def build_row_from_card(card: dict) -> dict | None:
    """
    Собираем одну строку для вставки в таблицу wb_cards_dimensions.
    Если нет габаритов — возвращаем None.
    """
    nm_id = card.get("nmID")
    vendor_code = card.get("vendorCode")
    brand = card.get("brand")
    object_name = card.get("object") or card.get("objectName")

    dimensions = card.get("dimensions") or {}

    length = dimensions.get("length")
    width = dimensions.get("width")
    height = dimensions.get("height")
    weight_brutto = dimensions.get("weightBrutto")
    updated_at_wb = card.get("updatedAt")

    # если габариты не заданы — пропускаем
    if not all([length, width, height]):
        return None

    try:
        length = float(length)
        width = float(width)
        height = float(height)
    except (TypeError, ValueError):
        return None

    volume_liters = (length * width * height) / 1000.0  # см³ → литры

    row = {
        "nm_id": nm_id,
        "vendor_code": vendor_code,
        "brand": brand,
        "object_name": object_name,
        "length_cm": round(length, 2),
        "width_cm": round(width, 2),
        "height_cm": round(height, 2),
        "weight_brutto_kg": float(weight_brutto) if weight_brutto is not None else None,
        "volume_liters": round(volume_liters, 3),
        "updated_at_wb": updated_at_wb,
        # fetched_at проставится default now() на стороне БД
    }

    return row


def refresh_supabase_table():
    """
    Полное обновление таблицы:
    1) очищаем wb_cards_dimensions
    2) вставляем новые данные батчами
    """
    print("Fetching cards from Wildberries...")

    rows: list[dict] = []

    for card in iter_wb_cards(limit=100):
        row = build_row_from_card(card)
        if row:
            rows.append(row)

    print(f"Total rows with dimensions: {len(rows)}")

    # Очищаем таблицу
    print("Deleting old data from wb_cards_dimensions...")
    supabase.table("wb_cards_dimensions").delete().neq("nm_id", 0).execute()

    # Вставляем батчами
    batch_size = 500
    total = len(rows)
    print("Inserting new data into wb_cards_dimensions...")

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        print(f"Inserting batch {i}..{i+len(batch)-1}")
        supabase.table("wb_cards_dimensions").insert(batch).execute()
        # Можно чуть притормозить, если переживаешь за лимиты
        time.sleep(0.2)

    print("Done.")


if __name__ == "__main__":
    refresh_supabase_table()
