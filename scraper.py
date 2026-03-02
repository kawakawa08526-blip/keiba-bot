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
    netkeibaの開催一覧から対象レースのrace_idを取得する
    当日 → 翌日 → 翌々日 の順で検索する（前日予想対応）
    """
    from datetime import timedelta
    today = datetime.today()

    # 当日・翌日・翌々日の順で検索
    for delta in [0, 1, 2]:
        target = today + timedelta(days=delta)
        date_str = target.strftime("%Y%m%d")
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

        except Exception as e:
            print(f"[ERROR] {date_str} race_id検索失敗: {e}")
            continue

    # 全て見つからなかった場合はフォールバック
    date_str = today.strftime("%Y%m%d")
    print(f"[WARNING] race_idが見つからなかった: {place}{race_num}R → フォールバック")
    return make_race_id(date_str[:4], place, 1, 1, race_num)

# ── 出馬表 ────────────────────────────────────────────
def get_shutuba(race_id: str) -> pd.DataFrame:
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        soup = BeautifulSoup(_get(url).text, "html.parser")
        horses = []
        for row in soup.select("tr.HorseList"):
            h = {}
            num = row.select_one(".Umaban")
            h["num"] = int(num.get_text(strip=True)) if num else 0

            name_tag = row.select_one(".HorseName a")
            h["name"] = name_tag.get_text(strip=True) if name_tag else ""
            h["horse_id"] = ""
            if name_tag and name_tag.get("href"):
                m = re.search(r"horse/(\w+)", name_tag["href"])
                if m: h["horse_id"] = m.group(1)

            barei = row.select_one(".Barei")
            if barei:
                t = barei.get_text(strip=True)
                h["sex"] = t[0] if t else ""
                h["age"] = int(t[1:]) if len(t) > 1 and t[1:].isdigit() else 0

            futan = row.select_one(".Futan")
            h["weight"] = float(futan.get_text(strip=True)) if futan else 55.0

            jockey = row.select_one(".Jockey a")
            h["jockey"] = jockey.get_text(strip=True) if jockey else ""
            h["jockey_id"] = ""
            if jockey and jockey.get("href"):
                m = re.search(r"jockey/(\w+)", jockey["href"])
                if m: h["jockey_id"] = m.group(1)

            trainer = row.select_one(".Trainer a")
            h["trainer"] = trainer.get_text(strip=True) if trainer else ""

            hw = row.select_one(".Weight")
            if hw:
                t = hw.get_text(strip=True)
                m = re.search(r"(\d+)", t)
                h["horse_weight"] = int(m.group(1)) if m else 0
                m2 = re.search(r"\(([+-]?\d+)\)", t)
                h["weight_diff"] = int(m2.group(1)) if m2 else 0
            else:
                h["horse_weight"] = 480
                h["weight_diff"] = 0

            if h["num"] > 0:
                horses.append(h)

        return pd.DataFrame(horses)
    except Exception as e:
        print(f"[SCRAPER ERROR] 出馬表: {e}")
        return pd.DataFrame()

# ── レース情報 ────────────────────────────────────────
def get_race_info(race_id: str) -> dict:
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    info = {
        "race_id": race_id,
        "race_name": f"レース{race_id[-2:]}",
        "distance": 1600,
        "surface": "芝",
        "direction": "右",
        "track_condition": "良",
        "race_class": "未勝利",
    }
    try:
        soup = BeautifulSoup(_get(url).text, "html.parser")

        title = soup.select_one(".RaceName")
        if title: info["race_name"] = title.get_text(strip=True)

        d1 = soup.select_one(".RaceData01")
        if d1:
            t = d1.get_text()
            m = re.search(r"(\d{3,4})m", t)
            if m: info["distance"] = int(m.group(1))
            info["surface"] = "芝" if "芝" in t else "ダート"
            info["direction"] = "右" if "右" in t else ("左" if "左" in t else "直線")

        d2 = soup.select_one(".RaceData02")
        if d2:
            t2 = d2.get_text()
            for cond in ["不良", "重", "稍重", "良"]:
                if cond in t2:
                    info["track_condition"] = cond
                    break
            for cls in ["G1", "G2", "G3", "オープン", "3勝", "2勝", "1勝", "未勝利", "新馬"]:
                if cls in t2:
                    info["race_class"] = cls
                    break
    except Exception as e:
        print(f"[SCRAPER ERROR] レース情報: {e}")
    return info

# ── オッズ ────────────────────────────────────────────
def get_odds(race_id: str) -> dict:
    url = f"https://race.netkeiba.com/odds/index.html?race_id={race_id}&type=b1"
    try:
        soup = BeautifulSoup(_get(url).text, "html.parser")
        odds = {}
        for row in soup.select("tr"):
            cols = row.select("td")
            if len(cols) >= 3:
                n = cols[0].get_text(strip=True)
                o = cols[2].get_text(strip=True)
                if n.isdigit() and re.match(r"\d+\.\d+", o):
                    odds[int(n)] = float(o)
        return odds
    except Exception as e:
        print(f"[SCRAPER ERROR] オッズ: {e}")
        return {}

# ── 馬の過去成績 ──────────────────────────────────────
def get_horse_history(horse_id: str, limit: int = 15) -> pd.DataFrame:
    url = f"https://db.netkeiba.com/horse/{horse_id}/"
    try:
        soup = BeautifulSoup(_get(url).text, "html.parser")
        records = []
        table = soup.select_one("table.race_table_01")
        if not table:
            return pd.DataFrame()

        for row in table.select("tr")[1:limit+1]:
            cols = row.select("td")
            if len(cols) < 22:
                continue
            r = {}
            try:
                r["rank"]     = cols[11].get_text(strip=True)
                dist_txt      = cols[7].get_text(strip=True)
                dm = re.search(r"\d+", dist_txt)
                r["distance"] = int(dm.group()) if dm else 0
                r["surface"]  = "芝" if "芝" in dist_txt else "ダート"
                r["track"]    = cols[9].get_text(strip=True)
                c1 = cols[20].get_text(strip=True) if len(cols) > 20 else ""
                c4 = cols[21].get_text(strip=True) if len(cols) > 21 else ""
                r["corner1"]  = int(c1) if c1.isdigit() else None
                r["corner4"]  = int(c4) if c4.isdigit() else None
                l3 = cols[22].get_text(strip=True) if len(cols) > 22 else ""
                r["last3f"]   = float(l3) if re.match(r"\d+\.\d+", l3) else None
                records.append(r)
            except:
                continue

        time.sleep(0.5)
        return pd.DataFrame(records)
    except Exception as e:
        print(f"[SCRAPER ERROR] 馬歴 ({horse_id}): {e}")
        return pd.DataFrame()


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
