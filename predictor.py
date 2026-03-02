"""
予想エンジン（全馬券種・資金配分・人気馬判定・馬名入り）
"""

import pandas as pd
import numpy as np
import pickle, os, json, glob

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import cross_val_score

MODEL_PATH  = "model/keiba_model.pkl"
DATA_DIR    = "data"

FEATURE_COLS = [
    "style_adv",        # 展開有利度
    "odds_score",       # オッズ逆数（人気度）
    "gate_score",       # 枠順スコア
    "weight_score",     # 斤量スコア
    "weight_diff_score",# 馬体重変動スコア
    "track_score",      # 馬場状態スコア
    "surface_score",    # 芝/ダート適性
    "distance_norm",    # 距離（正規化）
    "age_score",        # 年齢スコア
    "class_score",      # クラススコア
]

# ── 特徴量構築 ────────────────────────────────────────
def build_features(horses_df: pd.DataFrame, race_info: dict,
                   odds: dict, scenario: dict) -> pd.DataFrame:
    distance   = race_info.get("distance", 1600)
    surface    = race_info.get("surface", "芝")
    track_cond = race_info.get("track_condition", "良")
    race_class = race_info.get("race_class", "未勝利")

    track_score_map  = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
    class_score_map  = {
        "新馬": 1, "未勝利": 2, "1勝": 3, "2勝": 4, "3勝": 5,
        "オープン": 6, "G3": 7, "G2": 8, "G1": 9
    }
    track_sc  = track_score_map.get(track_cond, 0)
    class_sc  = class_score_map.get(race_class, 3)
    dist_norm = distance / 3600.0  # 最大3600mで正規化

    adv_df = scenario.get("horses_with_adv", horses_df)
    adv_map = dict(zip(adv_df["num"], adv_df["style_adv"])) if "style_adv" in adv_df.columns else {}

    rows = []
    for _, r in horses_df.iterrows():
        num = r["num"]
        odd = odds.get(num, 20.0)
        odds_score = 1.0 / odd if odd > 0 else 0.05

        gate = num
        if surface == "芝" and distance <= 1400:
            gate_score = 1.0 - (gate - 1) * 0.04
        elif surface == "ダート":
            gate_score = 0.9 + (gate - 1) * 0.01
        else:
            gate_score = 1.0 - (gate - 1) * 0.02
        gate_score = max(0.5, min(1.3, gate_score))

        w = r.get("weight", 55.0)
        weight_score = (55.0 - w) * 0.05 + 1.0

        wd = r.get("weight_diff", 0)
        wds = 0.1 if abs(wd) <= 4 else (-0.2 if abs(wd) > 10 else 0.0)

        surface_score = 0.0
        style = r.get("running_style", "不明")
        if track_cond in ["重", "不良"] and style in ["逃げ", "先行"]:
            surface_score = 0.25
        elif track_cond in ["重", "不良"] and style == "追込":
            surface_score = -0.25

        age = r.get("age", 4)
        age_score = 1.0 if age in [3, 4, 5] else (0.9 if age == 6 else 0.75)

        rows.append({
            "num":              num,
            "name":             r.get("name", ""),
            "running_style":    style,
            "odds":             odd,
            "style_adv":        adv_map.get(num, 1.0),
            "odds_score":       odds_score,
            "gate_score":       gate_score,
            "weight_score":     weight_score,
            "weight_diff_score": wds,
            "track_score":      track_sc,
            "surface_score":    surface_score,
            "distance_norm":    dist_norm,
            "age_score":        age_score,
            "class_score":      class_sc,
        })

    return pd.DataFrame(rows)

def score_rule(df: pd.DataFrame) -> pd.DataFrame:
    weights = {
        "style_adv": 0.28, "odds_score": 0.22, "gate_score": 0.14,
        "weight_score": 0.08, "weight_diff_score": 0.08, "track_score": 0.05,
        "surface_score": 0.05, "distance_norm": 0.03, "age_score": 0.04, "class_score": 0.03,
    }
    df = df.copy()
    df["rule_score"] = sum(df[col] * w for col, w in weights.items())
    mn, mx = df["rule_score"].min(), df["rule_score"].max()
    df["rule_score_norm"] = (df["rule_score"] - mn) / (mx - mn) if mx > mn else 0.5
    return df

def train_model(data_records: list) -> None:
    os.makedirs("model", exist_ok=True)
    rows = [{"features": d["features"], "label": 1 if int(d["rank"]) <= 5 else 0} for d in data_records]
    if not rows: return
    df = pd.DataFrame([r["features"] for r in rows])
    df["label"] = [r["label"] for r in rows]
    available = [c for c in FEATURE_COLS if c in df.columns]
    X, y = df[available].fillna(0), df["label"]
    model = GradientBoostingClassifier(n_estimators=300, learning_rate=0.04, max_depth=4, random_state=42)
    model.fit(X, y)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "features": available}, f)

def predict_ml(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ml_score"] = 0.5
    if not os.path.exists(MODEL_PATH): return df
    with open(MODEL_PATH, "rb") as f:
        p = pickle.load(f)
    X = df[p["features"]].fillna(0)
    proba = p["model"].predict_proba(X)
    df["ml_score"] = proba[:, 1] if proba.shape[1] > 1 else proba[:, 0]
    return df

def rank_horses(features_df: pd.DataFrame) -> pd.DataFrame:
    df = score_rule(features_df)
    df = predict_ml(df)
    use_ml = os.path.exists(MODEL_PATH)
    df["final_score"] = (df["rule_score_norm"] * 0.35 + df["ml_score"] * 0.65) if use_ml else df["rule_score_norm"]
    # 期待値計算 (スコア / オッズの逆数)
    df["expect_value"] = df["final_score"] * df["odds"]
    df = df.sort_values("final_score", ascending=False).reset_index(drop=True)
    df["pred_rank"] = range(1, len(df) + 1)
    return df

# ── 買い目生成 & シミュレーター ──────────────────────────
def generate_bets(ranked: pd.DataFrame, budget: int = 0) -> dict:
    if len(ranked) < 2: return {}
    
    n = min(5, len(ranked))
    top = ranked.head(n).to_dict('records')
    h1 = top[0]
    h2 = top[1] if n > 1 else None
    h3 = top[2] if n > 2 else None
    h4 = top[3] if n > 3 else None
    h5 = top[4] if n > 4 else None
    
    def fmt(h): return f"{int(h['num'])}({h['name']})"
    
    bets = {}

    # 単勝・複勝
    bets["単勝"] = [fmt(h1)]
    bets["複勝"] = [fmt(h) for h in [h1, h2] if h]

    # 馬連（◎軸 → 相手）
    if h2:
        umaren = [f"{fmt(h1)}-{fmt(h)}" for h in [h2, h3, h4] if h]
        bets["馬連"] = umaren

    # ワイド（◎○から相手へ）
    if h2 and h3:
        bets["ワイド"] = [f"{fmt(h1)}-{fmt(h2)}", f"{fmt(h1)}-{fmt(h3)}", f"{fmt(h2)}-{fmt(h3)}"]

    # 馬単（◎→相手、相手→◎）
    if h2:
        umatan = []
        for h in [h2, h3]:
            if h:
                umatan.append(f"{fmt(h1)}→{fmt(h)}")
                umatan.append(f"{fmt(h)}→{fmt(h1)}")
        bets["馬単"] = umatan

    # 3連複（上位4頭ボックス）
    if h2 and h3:
        sanfuku = [f"{fmt(h1)}-{fmt(h2)}-{fmt(h3)}"]
        if h4:
            sanfuku.append(f"{fmt(h1)}-{fmt(h2)}-{fmt(h4)}")
            sanfuku.append(f"{fmt(h1)}-{fmt(h3)}-{fmt(h4)}")
        bets["3連複"] = sanfuku

    # 3連単（◎軸フォーメーション）
    if h2 and h3:
        santan = [f"{fmt(h1)}→{fmt(h2)}→{fmt(h3)}", f"{fmt(h1)}→{fmt(h3)}→{fmt(h2)}"]
        if h4:
            santan.append(f"{fmt(h1)}→{fmt(h2)}→{fmt(h4)}")
            santan.append(f"{fmt(h2)}→{fmt(h1)}→{fmt(h3)}")
        bets["3連単"] = santan

    if budget > 0:
        # 簡易的な資金配分（スコアが高い順に厚く）
        sim = []
        total_points = sum(h['final_score'] for h in top)
        for h in top:
            amount = int((h['final_score'] / total_points) * budget // 100 * 100)
            if amount >= 100:
                sim.append(f"  {fmt(h)} の単勝に {amount}円")
        bets["💰【俺ならこう張るぜ】"] = sim if sim else ["予算が少なすぎるぜ、旦那！"]

    return bets

# ── レポート生成 ──────────────────────────────────────
def format_report(ranked: pd.DataFrame, bets: dict, race_info: dict, pace_report: str) -> str:
    marks = ["◎", "○", "▲", "△", "穴"]
    lines = []
    
    rname, dist, surf = race_info.get("race_name", ""), race_info.get("distance", ""), race_info.get("surface", "")
    track, cls = race_info.get("track_condition", "良"), race_info.get("race_class", "")

    lines.append("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥")
    lines.append(f"🏇 {rname}  【{cls}】")
    lines.append(f"   {surf}{dist}m / 馬場:{track}")
    lines.append("🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥\n")
    
    # 人気馬判定
    top_fav = ranked.loc[ranked['odds'].idxmin()] if not ranked.empty else None
    if top_fav is not None and top_fav['pred_rank'] > 3:
        lines.append(f"⚠️ 【危ない人気馬】\n   1番人気の{top_fav['name']}だが、データ上は少し怪しいと見てるぜ。外して高配当を狙うのも手だな。\n")
    elif top_fav is not None and top_fav['pred_rank'] == 1:
        lines.append(f"✅ 【信頼の人気馬】\n   1番人気の{top_fav['name']}は、データ的にも盤石だ。軸にするならこいつだな。\n")

    lines.append("🎯【俺の印だ】")
    for i, (_, row) in enumerate(ranked.head(5).iterrows()):
        mark = marks[i]
        lines.append(f"  {mark} {int(row['num']):2d}番 {row['name']:<10} [{row['running_style']}] {row['odds']:.1f}倍")

    lines.append(f"\n👊 {ranked.iloc[0]['name']}、こいつが今回一番気になる馬だな。")
    lines.append("   俺の勘が騒いでるぜ。あとは旦那次第だ！\n")
    lines.append(pace_report + "\n")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("💰【買い目シミュレーター】")
    
    for btype, combos in bets.items():
        if btype.startswith("💰"): continue
        lines.append(f"  {btype}：{' / '.join(combos)}")
    
    if "💰【俺ならこう張るぜ】" in bets:
        lines.append("\n" + "💰【俺ならこう張るぜ】")
        lines.extend(bets["💰【俺ならこう張るぜ】"])
        lines.append("\n⚠️ 確実性はねぇ、最後は旦那の判断で頼むぜ！")

    lines.append("━━━━━━━━━━━━━━━━━━━━\n")
    lines.append("最後に一つだけ言っとくぜ旦那…")
    lines.append("勝負はあくまで自己責任だ。無理のない範囲で楽しもうぜ！")
    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    lines.append("⚠️【免責事項】")
    lines.append("・これはAIによる参考予想だぜ。的中を保証するもんじゃねぇ。")
    lines.append("・馬券の購入は必ず自己責任で頼むぜ旦那！")
    lines.append("・20歳未満の馬券購入は法律で禁止されてるぜ。")
    lines.append("・ギャンブルにのめり込むなよ旦那。生活を壊したら元も子もねぇ。")
    lines.append("・困ったときは→ ギャンブル等依存症相談窓口：0570-004-978")
    lines.append("━━━━━━━━━━━━━━━━━━━━")
    
    return "\n".join(lines)
