"""
成績記録・回収率集計エンジン（博徒AI版）
予想保存・結果記録・過去照合・的中率集計 対応
"""

import json
import os
import re
from datetime import datetime, timedelta

HISTORY_DIR  = "data"
HISTORY_FILE = os.path.join(HISTORY_DIR, "bet_history.json")

def _load_history():
    os.makedirs(HISTORY_DIR, exist_ok=True)
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def _save_history(history):
    os.makedirs(HISTORY_DIR, exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def save_prediction(race_id, place, race_num, ranked_top, bets):
    history = _load_history()
    today = datetime.today().strftime("%Y%m%d")
    if today not in history:
        history[today] = {}
    history[today][race_id] = {
        "place": place,
        "race_num": race_num,
        "ranked_top": ranked_top,
        "bets": bets,
        "timestamp": datetime.now().strftime("%H:%M:%S"),
        "result": None,
        "honmei_rank": None,
    }
    _save_history(history)

def update_result(race_id, result_dict, date_str=None):
    history = _load_history()
    if date_str is None:
        date_str = datetime.today().strftime("%Y%m%d")

    # 指定日付 → 全日付の順で検索
    search_dates = [date_str] + [d for d in history.keys() if d != date_str]
    for d in search_dates:
        if d in history and race_id in history[d]:
            ranked_top = history[d][race_id].get("ranked_top", [])
            honmei_rank = None
            if ranked_top:
                honmei_num = ranked_top[0]["num"]
                honmei_rank = result_dict.get(honmei_num, None)
            history[d][race_id]["result"] = result_dict
            history[d][race_id]["honmei_rank"] = honmei_rank
            _save_history(history)
            return True
    return False

def parse_date_str(text):
    """
    「3/1」「3月1日」「20260301」などを "%Y%m%d" に変換
    年が省略された場合は今年を使用
    """
    text = text.strip()
    year = datetime.today().year

    # 20260301 形式
    m = re.search(r"(\d{8})", text)
    if m:
        return m.group(1)

    # 3/1 or 03/01 形式
    m = re.search(r"(\d{1,2})[/／](\d{1,2})", text)
    if m:
        return f"{year}{int(m.group(1)):02d}{int(m.group(2)):02d}"

    # 3月1日 形式
    m = re.search(r"(\d{1,2})月(\d{1,2})日?", text)
    if m:
        return f"{year}{int(m.group(1)):02d}{int(m.group(2)):02d}"

    return None

def get_daily_report(date_str=None):
    if date_str is None:
        date_str = datetime.today().strftime("%Y%m%d")

    history = _load_history()
    if date_str not in history or not history[date_str]:
        return (
            "まだ今日の勝負データがねぇみたいだぜ、旦那。\n"
            "まずはレースを聞いてくれ！"
        )

    races = history[date_str]
    total = len(races)
    tekichu = 0
    inshoku = 0
    result_count = 0

    lines = []
    lines.append("🔥🔥 本日の戦績報告 🔥🔥")
    lines.append(f"📅 {date_str[:4]}/{date_str[4:6]}/{date_str[6:]}")
    lines.append(f"🎯 参戦レース数：{total}戦\n")

    marks_label = ["◎", "○", "▲", "△", "穴"]
    for rid, data in races.items():
        place       = data.get("place", "")
        rnum        = data.get("race_num", "")
        ts          = data.get("timestamp", "")
        honmei_rank = data.get("honmei_rank", None)

        lines.append(f"━━ {place}{rnum}R（{ts}）━━")
        ranked = data.get("ranked_top", [])
        for i, h in enumerate(ranked):
            mark = marks_label[i] if i < len(marks_label) else " "
            lines.append(f"  {mark} {int(h['num'])}番 {h['name']}  {h['odds']}倍")

        if honmei_rank is not None:
            result_count += 1
            if honmei_rank == 1:
                tekichu += 1
                inshoku += 1
                lines.append(f"  🎉 ◎が{honmei_rank}着！的中だぜ旦那！")
            elif honmei_rank <= 3:
                inshoku += 1
                lines.append(f"  😤 ◎は{honmei_rank}着…惜しかったぜ旦那！")
            else:
                lines.append(f"  💀 ◎は{honmei_rank}着…外れたな旦那…")
        else:
            lines.append("  ⏳ 結果待ち…")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("📊【本日の成績集計】")
    if result_count > 0:
        tekichu_rate = int(tekichu / result_count * 100)
        inshoku_rate = int(inshoku / result_count * 100)
        lines.append(f"  結果確定：{result_count}レース")
        lines.append(f"  ◎1着的中：{tekichu}回（{tekichu_rate}%）")
        lines.append(f"  ◎3着内：{inshoku}回（{inshoku_rate}%）")
        if tekichu_rate >= 30:
            lines.append("  旦那、今日は調子いいぜ！このまま攻めろ！🔥")
        elif tekichu_rate >= 20:
            lines.append("  まずまずだぜ旦那。明日も勝負しようぜ！😏")
        else:
            lines.append("  今日は厳しかったな旦那…明日巻き返そうぜ！💪")
    else:
        lines.append("  まだ結果が出てねぇぜ。レース後にもう一度確認してくれ！")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️【免責事項】")
    lines.append("・これはAIによる参考予想の的中率だぜ。")
    lines.append("・的中を保証するもんじゃねぇ。馬券は自己責任で頼むぜ旦那！")
    lines.append("・20歳未満の馬券購入は法律で禁止されてるぜ。")
    lines.append("・ギャンブルにのめり込むなよ旦那。生活を壊したら元も子もねぇ。")
    lines.append("・困ったときは→ ギャンブル等依存症相談窓口：0570-004-978")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)

def get_collation_report(date_str, result_fetcher):
    """
    過去日付のAI予想と結果を照合して表示
    result_fetcher: race_idを受け取りresult_dictを返す関数
    """
    history = _load_history()

    if date_str not in history or not history[date_str]:
        return (
            f"旦那、{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}の\n"
            "予想データがねぇみたいだぜ。\n"
            "その日に予想したレースがないと照合できねぇ！"
        )

    races = history[date_str]
    tekichu = 0
    inshoku = 0
    total = 0
    marks_label = ["◎", "○", "▲", "△", "穴"]

    lines = []
    d = date_str
    lines.append(f"🔥【{d[:4]}/{d[4:6]}/{d[6:]} 予想 vs 結果 照合】🔥")
    lines.append("")

    for race_id, data in races.items():
        place   = data.get("place", "")
        rnum    = data.get("race_num", "")
        ranked  = data.get("ranked_top", [])
        total += 1

        lines.append(f"━━ 🏇 {place}{rnum}R ━━")

        # 結果を取得（既に記録済みならそれを使う）
        result = data.get("result")
        if not result:
            try:
                result = result_fetcher(race_id)
                if result:
                    # 記録も更新
                    honmei_rank = None
                    if ranked:
                        honmei_num = ranked[0]["num"]
                        honmei_rank = result.get(honmei_num)
                    history[date_str][race_id]["result"] = result
                    history[date_str][race_id]["honmei_rank"] = honmei_rank
            except:
                result = {}

        # 各馬の結果を表示
        for i, h in enumerate(ranked):
            mark = marks_label[i] if i < len(marks_label) else " "
            num  = int(h["num"])
            name = h["name"]
            odds = h["odds"]
            rank = result.get(num) if result else None
            rank_str = f"{rank}着" if rank else "不明"

            if rank == 1:
                lines.append(f"  {mark} {num}番 {name}({odds}倍) → {rank_str} 🎉")
            elif rank and rank <= 3:
                lines.append(f"  {mark} {num}番 {name}({odds}倍) → {rank_str} 😤")
            else:
                lines.append(f"  {mark} {num}番 {name}({odds}倍) → {rank_str}")

        # 本命の結果
        honmei_rank = data.get("honmei_rank")
        if not honmei_rank and result and ranked:
            honmei_rank = result.get(ranked[0]["num"])

        if honmei_rank == 1:
            tekichu += 1
            inshoku += 1
            lines.append("  ✅ ◎本命1着！的中だぜ旦那！🎉")
        elif honmei_rank and honmei_rank <= 3:
            inshoku += 1
            lines.append(f"  🔸 ◎本命{honmei_rank}着。惜しかったぜ！")
        elif honmei_rank:
            lines.append(f"  ❌ ◎本命{honmei_rank}着。外れたな旦那…")
        else:
            lines.append("  ⏳ 結果データなし")

        lines.append("")

    # 保存
    _save_history(history)

    # 集計
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("📊【照合結果サマリー】")
    lines.append(f"  参戦レース：{total}戦")
    if total > 0:
        t_rate = int(tekichu / total * 100)
        i_rate = int(inshoku / total * 100)
        lines.append(f"  ◎1着的中：{tekichu}回（{t_rate}%）")
        lines.append(f"  ◎3着内　：{inshoku}回（{i_rate}%）")
        if t_rate >= 35:
            lines.append("  旦那！この日は神がかってたぜ！🔥🔥")
        elif t_rate >= 20:
            lines.append("  まずまずの結果だぜ旦那。続けていこうぜ！😏")
        else:
            lines.append("  厳しい日だったな旦那…でも諦めんなよ！💪")

    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️ これはAI予想の的中率だぜ。馬券は自己責任で頼むぜ旦那！")
    lines.append("・20歳未満の馬券購入は法律で禁止されてるぜ。")
    lines.append("・困ったときは→ ギャンブル等依存症相談窓口：0570-004-978")
    lines.append("━━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)

def get_all_records_for_training():
    history = _load_history()
    records = []
    for date_str, races in history.items():
        for race_id, data in races.items():
            result = data.get("result")
            ranked = data.get("ranked_top", [])
            if result and ranked:
                for i, h in enumerate(ranked):
                    records.append({
                        "race_id": race_id,
                        "num": h["num"],
                        "rank": result.get(h["num"], 99),
                        "pred_rank": i + 1,
                    })
    return records
