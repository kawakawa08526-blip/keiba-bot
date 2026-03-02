"""
展開予想モジュール（強化版）
脚質推定 → ペース予測 → 有利不利スコア → 展開シナリオ生成
"""

import pandas as pd
import numpy as np

# ── 脚質推定 ──────────────────────────────────────────
def estimate_running_style(hist: pd.DataFrame) -> str:
    """
    過去成績のコーナー通過順位から脚質を推定
    corner1=スタート直後, corner4=最終コーナー
    """
    if hist.empty:
        return "不明"

    c1_list, c4_list = [], []
    for _, r in hist.iterrows():
        if r.get("corner1") is not None:
            c1_list.append(r["corner1"])
        if r.get("corner4") is not None:
            c4_list.append(r["corner4"])

    if not c4_list:
        return "不明"

    avg_c1 = np.mean(c1_list) if c1_list else np.mean(c4_list)
    avg_c4 = np.mean(c4_list)

    # 頭数は不明なので絶対値で判定
    if avg_c1 <= 2.0:
        return "逃げ"
    elif avg_c4 <= 4.0:
        return "先行"
    elif avg_c4 <= 8.0:
        return "差し"
    else:
        return "追込"

def add_running_styles(df: pd.DataFrame, histories: dict) -> pd.DataFrame:
    df = df.copy()
    df["running_style"] = df["horse_id"].apply(
        lambda hid: estimate_running_style(histories.get(hid, pd.DataFrame()))
    )
    return df

# ── ペース予測 ────────────────────────────────────────
def predict_pace(df: pd.DataFrame, race_info: dict) -> str:
    """
    逃げ・先行馬の数、距離、馬場から総合的にペースを判定
    """
    escapers   = len(df[df["running_style"] == "逃げ"])
    front      = len(df[df["running_style"] == "先行"])
    total      = len(df)
    distance   = race_info.get("distance", 1600)
    surface    = race_info.get("surface", "芝")
    track_cond = race_info.get("track_condition", "良")
    front_ratio = (escapers + front) / max(total, 1)

    # 基本判定
    if escapers >= 3 or front_ratio >= 0.45:
        pace = "ハイペース"
    elif escapers == 0 or (escapers == 1 and front <= 1):
        pace = "スローペース"
    elif escapers >= 2:
        pace = "ミドル〜ハイペース"
    else:
        pace = "ミドルペース"

    # 距離補正（長距離はスロー化しやすい）
    if distance >= 2400 and pace == "ミドルペース":
        pace = "スローペース"
    if distance <= 1200 and pace == "ミドルペース":
        pace = "ミドル〜ハイペース"

    # 重馬場補正（ペースが落ちやすい）
    if track_cond in ["重", "不良"] and pace == "ハイペース":
        pace = "ミドル〜ハイペース"

    return pace

# ── 有利度スコア ──────────────────────────────────────
def get_style_advantage(pace: str, style: str, distance: int,
                         surface: str, track_cond: str) -> float:
    """
    ペース × 脚質 × 距離 × 馬場 → 有利度スコア（0.1〜2.0）
    """
    score = 1.0

    # ── ペース × 脚質 ──
    pace_style_bonus = {
        "ハイペース":      {"逃げ": -0.4, "先行": -0.1, "差し": +0.4, "追込": +0.5, "不明": 0.0},
        "ミドル〜ハイペース": {"逃げ": -0.1, "先行": +0.2, "差し": +0.3, "追込": +0.1, "不明": 0.0},
        "ミドルペース":    {"逃げ": +0.1, "先行": +0.2, "差し": +0.2, "追込": +0.0, "不明": 0.0},
        "スローペース":    {"逃げ": +0.5, "先行": +0.4, "差し": -0.1, "追込": -0.4, "不明": 0.0},
    }
    score += pace_style_bonus.get(pace, {}).get(style, 0.0)

    # ── 距離 × 脚質 ──
    if distance <= 1400:
        dist_bonus = {"逃げ": +0.3, "先行": +0.2, "差し": -0.1, "追込": -0.2, "不明": 0.0}
    elif distance <= 2000:
        dist_bonus = {"逃げ": 0.0, "先行": +0.1, "差し": +0.2, "追込": +0.0, "不明": 0.0}
    else:
        dist_bonus = {"逃げ": -0.2, "先行": 0.0, "差し": +0.2, "追込": +0.3, "不明": 0.0}
    score += dist_bonus.get(style, 0.0)

    # ── 馬場状態補正 ──
    if track_cond in ["重", "不良"]:
        if style in ["逃げ", "先行"]:
            score += 0.2   # 内・前有利
        elif style == "追込":
            score -= 0.2

    # ── ダート補正（逃げ先行有利） ──
    if surface == "ダート" and style in ["逃げ", "先行"]:
        score += 0.15

    return max(0.1, min(2.0, round(score, 3)))

# ── 展開シナリオ構築 ──────────────────────────────────
def build_scenario(df: pd.DataFrame, race_info: dict) -> dict:
    distance   = race_info.get("distance", 1600)
    surface    = race_info.get("surface", "芝")
    track_cond = race_info.get("track_condition", "良")

    pace = predict_pace(df, race_info)

    # 脚質グループ
    groups = {}
    for style in ["逃げ", "先行", "差し", "追込", "不明"]:
        g = df[df["running_style"] == style]
        if not g.empty:
            groups[style] = list(zip(g["num"].tolist(), g["name"].tolist()))

    # 有利・不利脚質
    adv, disadv = [], []
    if pace == "ハイペース":
        adv, disadv = ["差し", "追込"], ["逃げ"]
        comment = "前が飛ばしそうな流れだな。後ろから差してくる馬が面白いかもしれねぇ。逃げ馬は苦しくなりそうだぜ。"
    elif pace == "スローペース":
        adv, disadv = ["逃げ", "先行"], ["追込"]
        comment = "ダラダラの流れになりそうだな。前にいる馬が粘り込みやすい展開だぜ。追込は届きにくいかもな。"
    elif pace == "ミドル〜ハイペース":
        adv, disadv = ["先行", "差し"], []
        comment = "淀みなく流れそうなレースだな。先行で脚を溜めてる差し馬が怪しいと見てるぜ。逃げ馬が踏ん張れるかも注目だな。"
    else:
        adv, disadv = ["先行", "差し"], []
        comment = "平均的な流れになりそうだな。実力が出やすいレースと見てるぜ。先行〜差しの馬が気になるな。"

    # 馬場コメント
    track_note = ""
    if track_cond == "不良":
        track_note = "⚠️ 不良馬場だぜ！前有利・内枠が有利になりやすい。道悪巧者にも注目だな。"
    elif track_cond == "重":
        track_note = "⚠️ 重馬場だな。先行有利になりやすく、内枠も活きそうだ。末脚自慢の馬は少し割引きかもな。"
    elif track_cond == "稍重":
        track_note = "📌 稍重だが、ほぼ良馬場と同じ感覚でいいと思うぜ。差しも決まりやすいかもな。"

    # 有利スコアを各馬に付与
    df = df.copy()
    df["style_adv"] = df.apply(
        lambda r: get_style_advantage(
            pace, r["running_style"], distance, surface, track_cond
        ), axis=1
    )

    return {
        "pace": pace,
        "advantage": adv,
        "disadvantage": disadv,
        "comment": comment,
        "track_note": track_note,
        "groups": groups,
        "horses_with_adv": df,
    }

# ── 展開テキスト生成 ──────────────────────────────────
def format_pace_report(scenario: dict) -> str:
    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("🎽【展開の読み】")
    lines.append(f"  ペース予想 ▶ 【{scenario['pace']}】")
    lines.append("")

    groups = scenario.get("groups", {})
    for style in ["逃げ", "先行", "差し", "追込"]:
        horses = groups.get(style, [])
        if horses:
            names = "・".join(f"⑤{num}{name}" for num, name in horses[:4])
            lines.append(f"  {style:<3}：{names}")

    lines.append("")
    lines.append(f"  💨 {scenario['comment']}")

    if scenario.get("track_note"):
        lines.append(f"  {scenario['track_note']}")

    adv = scenario.get("advantage", [])
    disadv = scenario.get("disadvantage", [])
    if adv:
        lines.append(f"  🔺 強い脚質：{'・'.join(adv)}")
    if disadv:
        lines.append(f"  🔻 苦しい脚質：{'・'.join(disadv)}")

    return "\n".join(lines)
