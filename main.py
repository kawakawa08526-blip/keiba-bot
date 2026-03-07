"""
競馬予想システム メイン
全馬券種・展開予想・的中記録・照合・自動学習 対応
"""

import json, os, time, re, threading
from datetime import datetime
from flask import Flask, request, abort

from scraper import (
    get_shutuba, get_race_info, get_horse_history,
    get_odds, get_race_result, make_race_id, find_race_id,
    PLACE_CODES
)
from pace_predictor import add_running_styles, build_scenario, format_pace_report
from predictor import (
    build_features, rank_horses, generate_bets,
    format_report, train_model
)
from recorder import (
    save_prediction, get_daily_report, update_result,
    get_all_records_for_training, get_collation_report, parse_date_str
)

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
    raise

app = Flask(__name__)

SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
TOKEN  = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
configuration = Configuration(access_token=TOKEN)
handler = WebhookHandler(SECRET)

os.makedirs("data", exist_ok=True)
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
    "「戦績」→ 今日の参戦レース＆的中率\n"
    "「結果 東京1R」→ レース結果を手動取得\n"
    "「照合 3/1」→ 過去の予想と結果を照合\n"
    "「予算3000 東京1R」→ 資金配分つき予想\n\n"
    "━━━━━━━━━━━━━━\n"
    "枠順確定後（前日の夕方以降）から\n"
    "予想が可能だ。さぁ、勝負しようぜ！"
)

DISCLAIMER = (
    "\n\n⚠️【免責事項】\n"
    "・これはAIによる参考予想だぜ。的中を保証するもんじゃねぇ。\n"
    "・馬券の購入は必ず自己責任で頼むぜ旦那！\n"
    "・20歳未満の馬券購入は法律で禁止されてるぜ。\n"
    "・ギャンブルにのめり込むなよ旦那。生活を壊したら元も子もねぇ。"
)


# ── ユーティリティ ──────────────────────────────────────

def zenkaku_to_hankaku(text):
    return text.translate(str.maketrans(
        '０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ',
        '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    ))

def parse_msg(text):
    """「東京1R」→ ("東京", 1) に変換"""
    clean = zenkaku_to_hankaku(text).replace(" ", "").replace("　", "").upper()
    for p in PLACE_NAMES:
        m = re.search(rf"{p}(\d{{1,2}})R?", clean)
        if m:
            rn = int(m.group(1))
            if 1 <= rn <= 12:
                return p, rn
    return None, None

def parse_budget_msg(text):
    """「予算3000 東京1R」→ ("東京", 1, 3000) に変換"""
    clean = zenkaku_to_hankaku(text).replace("　", " ")
    bm = re.search(r"予算\s*(\d+)", clean)
    budget = int(bm.group(1)) if bm else 0
    place, race_num = parse_msg(clean)
    return place, race_num, budget


# ── LINE メッセージ送信 ───────────────────────────────────

def reply_msg(reply_token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)]
            )
        )

def push_msg(user_id, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message_with_http_info(
            PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=text)]
            )
        )


# ── 予想処理 ─────────────────────────────────────────────

def predict_race(race_id, place="", race_num=0, budget=0):
    print(f"[INFO] 予想開始 race_id={race_id}")
    horses_df = get_shutuba(race_id)
    if horses_df.empty:
        return (
            "おい旦那、まだ出馬表が出てねぇみたいだぜ。\n"
            "枠順が確定してからもう一度頼むぜ！"
            + DISCLAIMER
        )
    race_info = get_race_info(race_id)
    race_info["place"] = place
    race_info["race_num"] = race_num
    odds = get_odds(race_id)

    # 馬歴取得（最大5頭のみ・タイムアウト対策）
    n_horses = len(horses_df)
    print(f"[INFO] {n_horses}頭の過去成績を取得中...")
    histories = {}
    count = 0
    for _, row in horses_df.iterrows():
        hid = row.get("horse_id", "")
        if hid and count < 5:  # 最大5頭に制限
            try:
                histories[hid] = get_horse_history(hid)
                count += 1
                time.sleep(0.2)
            except Exception:
                pass

    horses_df = add_running_styles(horses_df, histories)
    scenario   = build_scenario(horses_df, race_info)
    pace_report = format_pace_report(scenario)
    feat_df    = build_features(horses_df, race_info, odds, scenario)
    ranked     = rank_horses(feat_df)
    bets       = generate_bets(ranked, budget=budget)
    report     = format_report(ranked, bets, race_info, pace_report)

    marks = ["◎", "○", "▲", "△", "穴"]
    ranked_top = [
        {
            "num":  int(r["num"]),
            "name": r["name"],
            "mark": marks[i] if i < len(marks) else "",
            "odds": float(r["odds"]),
        }
        for i, (_, r) in enumerate(ranked.head(5).iterrows())
    ]
    save_prediction(race_id, place, race_num, ranked_top, bets)

    return report + DISCLAIMER


def do_predict(user_id, place, race_num, budget=0):
    """スレッドで予想を実行してプッシュ通知"""
    race_id = find_race_id(place, race_num)
    try:
        report = predict_race(race_id, place, race_num, budget=budget)
    except Exception as e:
        report = (
            f"すまねぇ旦那、データの取得にしくじっちまった…\n"
            f"もう一回試してくれ！\n({e})"
            + DISCLAIMER
        )
    push_msg(user_id, report)


def do_fetch_result(user_id, place, race_num):
    """結果を取得して記録・プッシュ通知"""
    race_id = find_race_id(place, race_num)
    try:
        result = get_race_result(race_id)
        if not result:
            push_msg(
                user_id,
                f"おい旦那、{place}{race_num}Rの結果がまだ出てねぇみたいだぜ。\n"
                "レース後しばらくしてからもう一度試してくれ！"
                + DISCLAIMER
            )
            return

        updated = update_result(race_id, result)
        sorted_result = sorted(result.items(), key=lambda x: x[1])
        medal = ["🥇", "🥈", "🥉"]
        lines = [
            f"🏁【{place}{race_num}R 結果】",
            "━━━━━━━━━━━━━━",
        ]
        for i, (num, rank) in enumerate(sorted_result[:3]):
            lines.append(f"  {medal[i]} {rank}着：{num}番")
        lines.append("━━━━━━━━━━━━━━")
        if updated:
            lines.append("予想データに結果を記録したぜ旦那！")
            lines.append("「戦績」で今日の的中率を確認してくれ！")
        else:
            lines.append("（このレースの予想データなし）")

        push_msg(user_id, "\n".join(lines) + DISCLAIMER)

    except Exception as e:
        push_msg(user_id, f"結果の取得にしくじっちまった…\nもう一回試してくれ！\n({e})")


def do_collation(user_id, date_str):
    """照合処理をスレッドで実行してプッシュ通知"""
    try:
        report = get_collation_report(date_str, get_race_result)
    except Exception as e:
        report = f"照合中にエラーが出ちまったぜ旦那…\nもう一度試してくれ！\n({e})"
    push_msg(user_id, report + DISCLAIMER)


def do_auto_train():
    """蓄積データで自動学習"""
    try:
        records = get_all_records_for_training()
        if len(records) < 50:
            print(f"[TRAIN] データ不足（{len(records)}件）。50件以上必要。")
            return
        train_model(records)
        print(f"[TRAIN] 学習完了！{len(records)}件で更新。")
    except Exception as e:
        print(f"[TRAIN ERROR] {e}")


# ── Flask ルーティング ────────────────────────────────────

@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


@handler.add(MessageEvent, message=TextMessageContent)
def on_message(event):
    text    = event.message.text.strip()
    user_id = event.source.user_id

    # ── ヘルプ ──
    if text in ["ヘルプ", "help", "使い方", "?", "へるぷ"]:
        reply_msg(event.reply_token, HELP)
        return

    # ── 戦績 ──
    if text in ["戦績", "成績", "本日の戦績", "今日の戦績"]:
        report = get_daily_report()
        reply_msg(event.reply_token, report + DISCLAIMER)
        return

    # ── 結果取得「結果 東京1R」──
    if text.startswith("結果"):
        rest = text.replace("結果", "").strip()
        place, race_num = parse_msg(rest)
        if place and race_num:
            reply_msg(
                event.reply_token,
                f"おう旦那、{place}{race_num}Rの結果を取ってくるぜ！\n少し待ってくれ…🔍"
            )
            t = threading.Thread(target=do_fetch_result, args=(user_id, place, race_num))
            t.daemon = True
            t.start()
        else:
            reply_msg(event.reply_token,
                "おっと旦那、レースを教えてくれ！\n例：「結果 東京1R」")
        return

    # ── 照合「照合 3/1」──
    if text.startswith(("照合", "対比", "検証")):
        rest = re.sub(r"^(照合|対比|検証)", "", text).strip()
        date_str = parse_date_str(rest)
        if date_str:
            d = date_str
            reply_msg(
                event.reply_token,
                f"おう旦那、{d[:4]}/{d[4:6]}/{d[6:]}の\n"
                "予想と結果を照合するぜ！\n少し待ってくれ…🔍"
            )
            t = threading.Thread(target=do_collation, args=(user_id, date_str))
            t.daemon = True
            t.start()
        else:
            reply_msg(event.reply_token,
                "おっと旦那、日付を教えてくれ！\n例：「照合 3/1」")
        return

    # ── 学習（手動トリガー）──
    if text in ["学習", "再学習", "train"]:
        reply_msg(
            event.reply_token,
            "おう旦那、蓄積データで学習を始めるぜ！\n"
            "データが少ないと学習できねぇかもしれねぇが…\n少し待ってくれ！🔥"
        )
        t = threading.Thread(target=do_auto_train)
        t.daemon = True
        t.start()
        return

    # ── 予算つきレース予想「予算3000 東京1R」──
    if "予算" in text:
        place, race_num, budget = parse_budget_msg(text)
        if place and race_num and budget > 0:
            reply_msg(
                event.reply_token,
                f"おう旦那、{place}{race_num}Rを予算{budget}円で勝負だな！\n"
                "今データを洗ってるから少し待ってくれ…🔥"
            )
            t = threading.Thread(
                target=do_predict,
                args=(user_id, place, race_num),
                kwargs={"budget": budget}
            )
            t.daemon = True
            t.start()
        else:
            reply_msg(event.reply_token,
                "おっと旦那、予算とレースを教えてくれ！\n例：「予算3000 東京1R」")
        return

    # ── 通常のレース予想「東京1R」──
    place, race_num = parse_msg(text)
    if place and race_num:
        reply_msg(
            event.reply_token,
            f"おう旦那、{place}{race_num}Rだな！\n"
            "今データを洗ってるから少し待ってくれ…🔥"
        )
        t = threading.Thread(target=do_predict, args=(user_id, place, race_num))
        t.daemon = True
        t.start()
    else:
        reply_msg(
            event.reply_token,
            f"おっと旦那、そいつは俺にはわからねぇな。\n"
            f"勝負したいレースを「東京1R」みたいに教えてくれ！\n\n{HELP}"
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
