"""
競馬予想システム メイン（最終版）
全馬券種・シミュレーター・人気馬判定・回収率集計 対応
"""

import argparse, json, os, time, glob, re, threading
from datetime import datetime
from flask import Flask, request, abort

from scraper import (
    get_shutuba, get_race_info, get_horse_history,
    get_odds, get_race_result, make_race_id, PLACE_CODES
)
from pace_predictor import add_running_styles, build_scenario, format_pace_report
from predictor import (
    build_features, rank_horses, generate_bets,
    format_report, train_model
)
from recorder import save_prediction, get_daily_report

# ── LINE Bot 関連のインポート ──
try:
    from linebot.v3 import WebhookHandler
    from linebot.v3.exceptions import InvalidSignatureError
    from linebot.v3.messaging import (
        Configuration, ApiClient, MessagingApi,
        ReplyMessageRequest, PushMessageRequest, TextMessage
    )
    from linebot.v3.webhooks import MessageEvent, TextMessageContent
except ImportError:
    print("[ERROR] line-bot-sdk がインストールされていません")

# ── Flask アプリケーション設定 (Render起動用にトップレベルに配置) ──
app = Flask(__name__)

SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
TOKEN  = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
configuration = Configuration(access_token=TOKEN)
handler = WebhookHandler(SECRET)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs("model", exist_ok=True)

PLACE_NAMES = list(PLACE_CODES.keys())
HELP = (
    "よぉ旦那、ようこそ！\n"
    "俺はAIで動く競馬予想の相棒だ。\n\n"
    "━━━━━━━━━━━━━━\n"
    "🎲【勝負するレースを教えてくれ】\n"
    "━━━━━━━━━━━━━━\n"
    "「東京1R」みたいに送ってくれりゃ\n"
    "俺がデータを洗って予想を弾き出すぜ。\n\n"
    "🏇【対応してる戦場】\n"
    "東京・中山・阪神・京都・中京\n"
    "小倉・新潟・福島・札幌・函館\n\n"
    "📊【俺が読み解くもの】\n"
    "✅ 展開の読み（ペース・脚質・有利不利）\n"
    "✅ 厳選の印（◎○▲△穴）\n"
    "✅ 危ない人気馬の判定\n"
    "✅ 全馬券種の買い目（単勝〜3連単）\n"
    "✅ 資金配分シミュレーター\n\n"
    "📋【その他のコマンド】\n"
    "「戦績」→ 今日の参戦レース一覧\n"
    "「予算3000 東京1R」→ 資金配分つき予想\n\n"
    "━━━━━━━━━━━━━━\n"
    "枠順確定後（前日の夕方以降）から\n"
    "予想が可能だ。さぁ、勝負しようぜ！"
)

# ── 共通処理 ──
def predict_race(race_id, place="", race_num=0, silent=False, budget=0):
    if not silent:
        print(f"\n🔍 予想開始 race_id={race_id}")
    horses_df = get_shutuba(race_id)
    if horses_df.empty:
        return "おい旦那、まだ出馬表が出てねぇみたいだぜ。\n枠順が確定してからもう一度頼むぜ！"
    race_info = get_race_info(race_id)
    race_info["place"] = place
    race_info["race_num"] = race_num
    odds = get_odds(race_id)
    # オッズが取得できない場合（前日など）はデフォルト値を設定
    if not odds:
        if not silent:
            print("⚠️ オッズが取得できませんでした。デフォルト値(10.0)で計算します。")
        for _, row in horses_df.iterrows():
            odds[int(row["num"])] = 10.0

    if not silent:
        print(f"📥 {len(horses_df)}頭の過去成績を取得中...")
    histories = {}
    for _, row in horses_df.iterrows():
        hid = row.get("horse_id", "")
        if hid:
            histories[hid] = get_horse_history(hid)
            time.sleep(0.6)
    horses_df = add_running_styles(horses_df, histories)
    scenario = build_scenario(horses_df, race_info)
    pace_report = format_pace_report(scenario)
    feat_df = build_features(horses_df, race_info, odds, scenario)
    ranked = rank_horses(feat_df)
    bets = generate_bets(ranked, budget=budget)
    report = format_report(ranked, bets, race_info, pace_report)

    # 戦績記録用に保存
    marks = ["◎", "○", "▲", "△", "穴"]
    ranked_top = []
    for i, (_, r) in enumerate(ranked.head(5).iterrows()):
        ranked_top.append({
            "num": int(r["num"]),
            "name": r["name"],
            "mark": marks[i] if i < len(marks) else "",
            "odds": float(r["odds"]),
        })
    save_prediction(race_id, place, race_num, ranked_top, bets)

    if not silent:
        print(report)
    return report

def reply_msg(reply_token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=text)])
        )

def push_msg(user_id, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message_with_http_info(
            PushMessageRequest(to=user_id, messages=[TextMessage(text=text)])
        )

def do_predict(user_id, place, race_num, budget=0):
    today = datetime.today().strftime("%Y%m%d")
    race_id = make_race_id(today[:4], place, 1, 1, race_num)
    try:
        report = predict_race(race_id, place, race_num, silent=True, budget=budget)
    except Exception as e:
        report = f"すまねぇ旦那、データの取得にしくじっちまった…\nもう一回試してくれ！\n({e})"
    push_msg(user_id, report)

def zenkaku_to_hankaku(text):
    """全角英数字を半角に変換"""
    return text.translate(str.maketrans(
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    ))

def parse_msg(text):
    """レース名を解析する（全角・半角・R有無に対応）"""
    clean = zenkaku_to_hankaku(text).replace(" ", "").replace("　", "").upper()
    for p in PLACE_NAMES:
        pattern = rf"{p}(\d{{1,2}})R?"
        m = re.search(pattern, clean)
        if m:
            rn = int(m.group(1))
            if 1 <= rn <= 12:
                return p, rn
    return None, None

def parse_budget_msg(text):
    """「予算3000 東京1R」のような予算付きメッセージを解析する"""
    clean = zenkaku_to_hankaku(text).replace("　", " ")
    budget_match = re.search(r"予算\s*(\d+)", clean)
    budget = int(budget_match.group(1)) if budget_match else 0
    place, race_num = parse_msg(clean)
    return place, race_num, budget

# ── 学習データ収集・学習（コマンドライン用） ──
def collect_date(date):
    print(f"\n📦 {date} の学習データ収集開始")
    all_records = []
    for place, code in PLACE_CODES.items():
        for kai in [1, 2]:
            for nichi in range(1, 9):
                for race_num in range(1, 13):
                    race_id = make_race_id(date[:4], place, kai, nichi, race_num)
                    results = get_race_result(race_id)
                    if not results:
                        continue
                    horses_df = get_shutuba(race_id)
                    if horses_df.empty:
                        continue
                    race_info = get_race_info(race_id)
                    odds = get_odds(race_id)
                    histories = {}
                    for _, r in horses_df.iterrows():
                        hid = r.get("horse_id", "")
                        if hid:
                            histories[hid] = get_horse_history(hid)
                            time.sleep(0.5)
                    horses_df = add_running_styles(horses_df, histories)
                    scenario = build_scenario(horses_df, race_info)
                    feat_df = build_features(horses_df, race_info, odds, scenario)
                    for _, f_row in feat_df.iterrows():
                        all_records.append({"features": f_row.to_dict(), "rank": results.get(int(f_row["num"]), 99)})
                    time.sleep(1.0)
    path = os.path.join(DATA_DIR, f"train_{date}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    print(f"💾 {path} ({len(all_records)}件)")

def run_train():
    all_data = []
    for path in sorted(glob.glob(os.path.join(DATA_DIR, "train_*.json"))):
        with open(path, encoding="utf-8") as f:
            batch = json.load(f)
        all_data.extend(batch)
    if not all_data:
        print("[ERROR] 学習データなし")
        return
    train_model(all_data)

# ── Flask ルーティング ──
@app.route("/callback", methods=["POST"])
def callback():
    sig = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def on_message(event):
    text = event.message.text.strip()

    # ── ヘルプ ──
    if text in ["ヘルプ", "help", "使い方", "?", "へるぷ"]:
        reply_msg(event.reply_token, HELP)
        return

    # ── 戦績コマンド ──
    if text in ["戦績", "成績", "本日の戦績", "今日の戦績"]:
        report = get_daily_report()
        reply_msg(event.reply_token, report)
        return

    # ── 予算つきレース予想 ──
    if "予算" in text:
        place, race_num, budget = parse_budget_msg(text)
        if place and race_num and budget > 0:
            reply_msg(event.reply_token,
                f"おう旦那、{place}{race_num}Rを予算{budget}円で勝負だな！\n"
                f"今データを洗ってるから少し待ってくれ…🔥"
            )
            t = threading.Thread(target=do_predict,
                args=(event.source.user_id, place, race_num),
                kwargs={"budget": budget})
            t.daemon = True
            t.start()
        else:
            reply_msg(event.reply_token,
                "おっと旦那、予算とレースを教えてくれ！\n"
                "例：「予算3000 東京1R」\n"
                "こんな感じで頼むぜ！"
            )
        return

    # ── 通常のレース予想 ──
    place, race_num = parse_msg(text)
    if place and race_num:
        reply_msg(event.reply_token,
            f"おう旦那、{place}{race_num}Rだな！\n"
            f"今データを洗ってるから少し待ってくれ…🔥"
        )
        t = threading.Thread(target=do_predict, args=(event.source.user_id, place, race_num))
        t.daemon = True
        t.start()
    else:
        reply_msg(event.reply_token,
            f"おっと旦那、そいつは俺にはわからねぇな。\n"
            f"勝負したいレースを「東京1R」みたいに教えてくれ！\n\n{HELP}"
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
