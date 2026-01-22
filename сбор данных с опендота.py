import requests
import csv
import time
import os
from datetime import datetime, timezone

#Параметры
DAILY_LIMIT = 2000          # лимит запросов в день (OpenDota)
REQUESTS_PER_MIN = 60       # лимит запросов в минуту
MATCHES_PER_REQUEST = 100   # по умолчанию API возвращает до 100 матчей

# Фильтр по времени (патч 7.39 — август 2025)
PATCH_7_39D_START = datetime(2025, 8, 5, 0, 0, 0, tzinfo=timezone.utc).timestamp()

# Фильтр по рангу 
MIN_RANK_TIER = 60

# Файл для сохранения
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(SCRIPT_DIR, "main.csv")


def fetch_matches(less_than_match_id=None):
    # Получаем список матчей с OpenDota API
    url = "https://api.opendota.com/api/publicMatches"
    params = {}
    if less_than_match_id:
        params["less_than_match_id"] = less_than_match_id

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Ошибка запроса: {response.status_code}")
            return []
    except Exception as e:
        print(f"Ошибка при подключении: {e}")
        return []


def main():
    print(f"Начинаем сбор (лимит {DAILY_LIMIT} запросов)")
    print(f"Файл для сохранения: {FILE_PATH}")

    # Загружаем уже собранные match_id
    existing_match_ids = set()
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    existing_match_ids.add(int(row["match_id"]))
                except:
                    pass
        print(f"Найден существующий файл, найдено {len(existing_match_ids)} матчей для пропуска.")

    # Открываем CSV для записи
    file_exists = os.path.exists(FILE_PATH)
    with open(FILE_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["match_id", "start_time", "radiant_win",
                      "radiant_team", "dire_team", "avg_rank_tier"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        total_collected = 0
        total_skipped = 0
        last_match_id = None

        # Основной цикл запросов
        for req_count in range(DAILY_LIMIT):
            matches = fetch_matches(less_than_match_id=last_match_id)
            if not matches:
                print("Нет данных, ожидаем 60 секунд...")
                time.sleep(60)
                continue

            for match in matches:
                start_time = match.get("start_time")
                match_id = match.get("match_id")

                # Пропуск старых матчей
                if not start_time or start_time < PATCH_7_39D_START:
                    continue

                # Пропуск дублей
                if match_id in existing_match_ids:
                    total_skipped += 1
                    continue

                avg_rank_tier = match.get("avg_rank_tier", 0)
                if avg_rank_tier < MIN_RANK_TIER:
                    continue

                writer.writerow({
                    "match_id": match_id,
                    "start_time": start_time,
                    "radiant_win": match.get("radiant_win"),
                    "radiant_team": match.get("radiant_team"),
                    "dire_team": match.get("dire_team"),
                    "avg_rank_tier": avg_rank_tier
                })
                existing_match_ids.add(match_id)
                total_collected += 1

            last_match_id = matches[-1]["match_id"]

            print(f"Собрано {total_collected} новых, пропущено {total_skipped}, запросов: {req_count + 1}/{DAILY_LIMIT}")

            # Соблюдаем лимит 60 запросов в минуту
            if (req_count + 1) % REQUESTS_PER_MIN == 0:
                print("Пауза 30 секунд (лимит в минуту)...")
                time.sleep(30)

        print("Достигнут дневной лимит. Сбор завершён.")


if __name__ == "__main__":
    main()
