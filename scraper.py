"""
競馬データ取得モジュール
netkeibaから出馬表・オッズ・馬歴・レース情報を取得
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime, timedelta

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

def find_race_id(place: str, race_num: int) -> str:
    """
    netkeibaのrace_idを取得する
    方法1: race_list.htmlから12桁IDを全抽出（最速・確実）
    方法2: shutuba.htmlに直接アクセスして出馬表＋日付を確認
    """
    today = datetime.today()
    place_code = PLACE_CODES.get(place, "")
    if not place_code:
        return make_race_id(today.strftime("%Y"), place, 1, 1, race_num)

    for delta in [0, 1, 2]:
        target = today + timedelta(days=delta)
        date_str = target.strftime("%Y%m%d")
        year = date_str[:4]
        month = str(int(date_str[4:6]))
        day   = str(int(date_str[6:8]))

        # 方法1: race_list.htmlから12桁IDを全抽出
        try:
            url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
            res = _get(url, sleep=1.0)
            ids = re.findall(r"\d{12}", res.text)
            # 場所コードとR番号が一致し、かつ年が正しいものだけ
            matched = [
                i for i in ids
                if i[:4] == year
                and i[4:6] == place_code
                and int(i[-2:]) == race_num
            ]
            if matched:
                race_id = list(dict.fromkeys(matched))[0]
                print(f"[INFO] 方法1でrace_id発見: {race_id}")
                return race_id
        except Exception as e:
            print(f"[ERROR] 方法1失敗: {e}")

        # 方法2: 総当たりでshutuba.htmlに直接アクセス＋日付確認
        print(f"[INFO] 方法2: 総当たり {place}{race_num}R {date_str}")
        date_checks = [date_str, f"{month}月{day}日", f"{month}/{day}"]
        for kai in range(1, 6):
            for nichi in range(1, 10):
                race_id = f"{year}{place_code}{kai:02d}{nichi:02d}{race_num:02d}"
                try:
                    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
                    res = _get(url, sleep=0.4)
                    soup = BeautifulSoup(res.text, "html.parser")
                    horse_rows = soup.select("tr[class*='HorseList']")
                    if not horse_rows or len(horse_rows) < 3:
                        continue
                    # ページ内に対象日付が含まれるか確認
                    if any(c in res.text for c in date_checks):
                        print(f"[INFO] 方法2でrace_id発見: {race_id} ({len(horse_rows)}頭) 日付OK")
                        return race_id
                except Exception:
                    continue

    date_str = today.strftime("%Y%m%d")
    print(f"[WARNING] race_idが見つからなかった: {place}{race_num}R → フォールバック")
    return make_race_id(date_str[:4], place, 1, 1, race_num)


def find_race_id_by_date(place: str, race_num: int, date_str: str) -> str:
    """過去日付のrace_idを取得"""
    place_code = PLACE_CODES.get(place, "")
    year = date_str[:4]
    try:
        url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={date_str}"
        res = _get(url, sleep=1.0)
        ids = re.findall(r"\d{12}", res.text)
        for race_id in ids:
            if race_id[:4] == year and race_id[4:6] == place_code and int(race_id[-2:]) == race_num:
                return race_id
    except Exception as e:
        print(f"[ERROR] find_race_id_by_date失敗: {e}")
    for kai in range(1, 6):
        for nichi in range(1, 10):
            race_id = f"{year}{place_code}{kai:02d}{nichi:02d}{race_num:02d}"
            try:
                url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
                res = _get(url, sleep=0.3)
                if "HorseList" in res.text:
                    return race_id
            except:
                continue
    return make_race_id(year, place, 1, 1, race_num)


def get_shutuba(race_id: str) -> pd.DataFrame:
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    try:
        res = _get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        horses = []
        all_rows = soup.select("tr[class*='HorseList']")
        print(f"[INFO] get_shutuba: {len(all_rows)}頭 race_id={race_id}")
        for row in all_rows:
            h = {}
            num = row.select_one("[class*='Umaban'], [class*='うまばん']")
            h["num"] = int(num.get_text(strip=True)) if num else 0

            name_tag = row.select_one("[class*='HorseName'] a, [class*='馬名'] a")
            if not name_tag:
                name_tag = row.select_one("td a[href*='horse']")
            h["name"] = name_tag.get_text(strip=True) if name_tag else ""
            h["horse_id"] = ""
            if name_tag and name_tag.get("href"):
                m = re.search(r"horse/(\w+)", name_tag["href"])
                if m:
                    h["horse_id"] = m.group(1)

            barei = row.select_one("[class*='Barei'], [class*='馬齢']")
            if barei:
                t = barei.get_text(strip=True)
                h["sex"] = t[0] if t else ""
                h["age"] = int(t[1:]) if len(t) > 1 and t[1:].isdigit() else 0
            else:
                h["sex"] = ""
                h["age"] = 0

            futan = row.select_one("[class*='Futan'], [class*='斤量']")
            h["weight"] = float(futan.get_text(strip=True)) if futan else 55.0

            jockey = row.select_one("[class*='Jockey'] a, [class*='騎手'] a")
            h["jockey"] = jockey.get_text(strip=True) if jockey else ""
            h["jockey_id"] = ""
            if jockey and jockey.get("href"):
                m = re.search(r"jockey/(\w+)", jockey["href"])
                if m:
                    h["jockey_id"] = m.group(1)

            trainer = row.select_one("[class*='Trainer'] a, [class*='調教師'] a")
            h["trainer"] = trainer.get_text(strip=True) if trainer else ""

            hw = row.select_one("[class*='Weight'], [class*='馬体重']")
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

        # レース名（複数セレクタで対応）
        for sel in [".RaceName", "[class*='RaceName']", "h1.RaceName", ".race_name"]:
            title = soup.select_one(sel)
            if title and title.get_text(strip=True):
                info["race_name"] = title.get_text(strip=True)
                break

        # ページ全体テキストから距離・馬場を抽出（最も確実）
        full_text = soup.get_text()

        # 距離
        m = re.search(r"(\d{3,4})m", full_text)
        if m:
            info["distance"] = int(m.group(1))

        # 芝/ダート
        info["surface"] = "芝" if "芝" in full_text[:3000] else "ダート"

        # 方向
        info["direction"] = "右" if "右" in full_text[:3000] else ("左" if "左" in full_text[:3000] else "直線")

        # 馬場状態
        for cond in ["不良", "重", "稍重", "良"]:
            if cond in full_text[:3000]:
                info["track_condition"] = cond
                break

        # クラス
        for cls in ["G1", "G2", "G3", "オープン", "3勝", "2勝", "1勝", "未勝利", "新馬"]:
            if cls in full_text[:5000]:
                info["race_class"] = cls
                break

        print(f"[INFO] race_info: {info['race_name']} {info['distance']}m {info['surface']} {info['race_class']}")
    except Exception as e:
        print(f"[SCRAPER ERROR] レース情報: {e}")
    return info


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


def get_horse_history(horse_id: str, limit: int = 5) -> pd.DataFrame:
    """馬の過去成績（直近5戦のみ取得して高速化）"""
    url = f"https://db.netkeiba.com/horse/{horse_id}/"
    try:
        soup = BeautifulSoup(_get(url, sleep=0.3).text, "html.parser")
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

        return pd.DataFrame(records)
    except Exception as e:
        print(f"[SCRAPER ERROR] 馬歴 ({horse_id}): {e}")
        return pd.DataFrame()


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
