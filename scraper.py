"""
競馬データ取得モジュール
netkeibaから出馬表・オッズ・馬歴・レース情報を取得
race_idを検索で正しく取得する
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
}

PLACE_CODES = {
    "札幌": "01", "函館": "02", "福島": "03", "新潟": "04",
    "東京": "05", "中山": "06", "中京": "07", "京都": "08",
    "阪神": "09", "小倉": "10"
}

def _get(url, sleep=0.8):
    time.sleep(sleep)
    res = requests.get(url, headers=HEADERS, timeout=15)
    res.encoding = "EUC-JP"
    return res

def make_race_id(date: str, place: str, kai: int, nichi: int, race_num: int) -> str:
    code = PLACE_CODES.get(place, "05")
    return f"{date}{code}{kai:02d}{nichi:02d}{race_num:02d}"

# ── race_idを検索して正しく取得（当日・翌日・翌々日対応）──
def find_race_id(place: str, race_num: int) -> str:
    """
    netkeibaのrace_idを取得する
    1. race_list.htmlからリンクを探す
    2. 見つからない場合は開催回・日を総当たりで直接確認
    """
    from datetime import timedelta
    today = datetime.today()
    place_code = PLACE_CODES.get(place, "")
    if not place_code:
        return make_race_id(today.strftime("%Y"), place, 1, 1, race_num)

    # 当日・翌日・翌々日の順で検索
    for delta in [0, 1, 2]:
        target = today + timedelta(days=delta)
        date_str = target.strftime("%Y%m%d")
        year = date_str[:4]

        # ── 方法1: race_list.htmlからリンク抽出 ──
        try:
            url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
            res = _get(url, sleep=1.0)
            text = res.text
            # 12桁のrace_idを全て抽出
            ids = re.findall(r'(\d{12})', text)
            for race_id in ids:
                if race_id[4:6] == place_code and int(race_id[-2:]) == race_num:
                    print(f"[INFO] 方法1でrace_id発見: {race_id}")
                    return race_id
        except Exception as e:
            print(f"[ERROR] 方法1失敗: {e}")

        # ── 方法2: 開催回・日を総当たりで直接確認 ──
        print(f"[INFO] 方法2: 総当たり検索開始 {place}{race_num}R {date_str}")
        for kai in range(1, 6):
            for nichi in range(1, 10):
                race_id = f"{year}{place_code}{kai:02d}{nichi:02d}{race_num:02d}"
                try:
                    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
                    res = _get(url, sleep=0.3)
                    if "HorseList" in res.text or "horse_info" in res.text:
                        print(f"[INFO] 方法2でrace_id発見: {race_id}")
                        return race_id
                except:
                    continue

    # フォールバック
    date_str = today.strftime("%Y%m%d")
    print(f"[WARNING] race_idが見つからなかった: {place}{race_num}R → フォールバック")
    return make_race_id(date_str[:4], place, 1, 1, race_num)

# ── 過去日付のrace_idを検索 ──────────────────────────
def find_race_id_by_date(place: str, race_num: int, date_str: str) -> str:
    """
    指定日付のrace_idを取得する（過去日付対応）
    date_str: "20260301" 形式
    """
    try:
        url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
        res = _get(url)
        soup = BeautifulSoup(res.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r"race_id=(\d{12})", href)
            if not m:
                continue
            race_id = m.group(1)
            place_code = PLACE_CODES.get(place, "")
            if race_id[4:6] == place_code and int(race_id[-2:]) == race_num:
                print(f"[INFO] race_id発見: {race_id}（{date_str}）")
                return race_id

        print(f"[WARNING] race_idが見つからなかった: {place}{race_num}R {date_str}")
        return make_race_id(date_str[:4], place, 1, 1, race_num)

    except Exception as e:
        print(f"[ERROR] {date_str} race_id検索失敗: {e}")
        return make_race_id(date_str[:4], place, 1, 1, race_num)
# ── 過去レース結果（学習用） ──────────────────────────
def get_race_result(race_id: str) -> dict:
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    try:
        soup = BeautifulSoup(_get(url).text, "html.parser")
        results = {}
        table = soup.select_one("table.RaceTable01")
        if not table:
            return {}
        for row in table.select("tr")[1:]:
            cols = row.select("td")
            if len(cols) >= 3:
                rk = cols[0].get_text(strip=True)
                nm = cols[2].get_text(strip=True)
                if rk.isdigit() and nm.isdigit():
                    results[int(nm)] = int(rk)
        return results
    except Exception as e:
        print(f"[SCRAPER ERROR] 結果 ({race_id}): {e}")
        return {}
