"""
writer.py — CSV ve Kaggle dataset dosyalarını yazar
"""

import csv
import json
import os
import datetime
import logging

log = logging.getLogger("writer")


# ──────────────────────────────────────────────────────────────────────────────
# Temel CSV yazıcı
# ──────────────────────────────────────────────────────────────────────────────
def write_csv(path: str, rows: list, fieldnames: list = None):
    if not rows:
        log.warning(f"Boş veri → {path} atlandı")
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fieldnames is None:
        fieldnames = list(dict.fromkeys(k for r in rows for k in r.keys()))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"  ✓ {os.path.basename(path)}  ({len(rows)} satır)")
    return len(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Sabit sütun şemaları
# ──────────────────────────────────────────────────────────────────────────────
SCHEMAS = {
    "standings": [
        "season_id", "position", "team_id", "team", "team_code",
        "played", "wins", "draws", "losses",
        "goals_for", "goals_against", "goal_diff", "points",
    ],
    "teams": [
        "team_id", "name", "short_name", "name_code",
        "manager", "stadium", "stadium_city", "capacity",
        "primary_color", "form_5", "avg_rating",
    ],
    "team_stats": [
        "team_id", "season_id", "matches",
        "goals_scored", "goals_conceded", "assists",
        "shots", "shots_on_target", "big_chances", "big_chances_created", "big_chances_missed",
        "avg_possession", "total_passes", "accurate_passes_pct",
        "accurate_long_balls_pct", "accurate_crosses_pct", "corners",
        "tackles", "interceptions", "clearances",
        "saves", "clean_sheets", "errors_leading_to_goal",
        "yellow_cards", "red_cards", "avg_rating",
        "duels_won_pct", "aerial_duels_won_pct",
        "successful_dribbles", "offsides", "fouls",
    ],
    "matches": [
        "match_id", "season_id", "round", "start_ts",
        "home_team_id", "home_team", "away_team_id", "away_team",
        "home_score", "away_score", "home_ht_score", "away_ht_score",
        "winner_code", "status", "status_code",
    ],
    "goals": [
        "match_id", "minute", "added_time",
        "scorer", "scorer_id", "assist", "assist_id",
        "goal_type", "is_home", "home_score", "away_score",
    ],
    "cards": [
        "match_id", "minute", "added_time",
        "player", "player_id", "card_type", "is_home",
    ],
    "substitutions": [
        "match_id", "minute",
        "player_in", "player_in_id", "player_out", "player_out_id", "is_home",
    ],
    "player_profiles": [
        "player_id", "name", "short_name", "position", "jersey_number",
        "height_cm", "preferred_foot", "dob_timestamp",
        "nationality", "nationality_iso2",
        "team_id", "market_value_eur", "market_value_cur",
    ],
    "player_stats": [
        "player_id", "team_id", "season_id",
        "appearances", "matches_started", "minutes_played",
        "goals", "assists", "expected_goals", "expected_assists",
        "goals_assists_sum", "penalty_goals", "free_kick_goals", "headed_goals",
        "left_foot_goals", "right_foot_goals",
        "total_shots", "shots_on_target", "shots_off_target",
        "big_chances_created", "big_chances_missed",
        "key_passes", "total_passes", "accurate_passes", "accurate_passes_percentage",
        "accurate_long_balls", "accurate_long_balls_percentage",
        "accurate_crosses", "accurate_crosses_percentage",
        "successful_dribbles", "successful_dribbles_percentage",
        "tackles", "tackles_won", "interceptions", "clearances",
        "total_duels_won", "total_duels_won_percentage",
        "ground_duels_won", "ground_duels_won_percentage",
        "aerial_duels_won", "aerial_duels_won_percentage",
        "saves", "clean_sheet", "goals_conceded", "penalty_save",
        "yellow_cards", "red_cards", "yellow_red_cards",
        "fouls", "was_fouled", "offsides", "dispossessed",
        "rating",
    ],
}


# ──────────────────────────────────────────────────────────────────────────────
# Toplu yazıcı
# ──────────────────────────────────────────────────────────────────────────────
def write_dataset(out_dir: str, tables: dict) -> dict:
    """
    tables = {
        "standings":      [...],
        "teams":          [...],
        "team_stats":     [...],
        "matches":        [...],
        "goals":          [...],
        "cards":          [...],
        "substitutions":  [...],
        "match_stats":    [...],   # dinamik sütunlar
        "player_profiles":[...],
        "player_stats":   [...],
    }
    Döndürür: {table_name: row_count}
    """
    counts = {}
    for name, rows in tables.items():
        path = os.path.join(out_dir, f"{name}.csv")
        schema = SCHEMAS.get(name)  # None → dinamik
        counts[name] = write_csv(path, rows, schema)
    return counts


# ──────────────────────────────────────────────────────────────────────────────
# Kaggle dataset-metadata.json
# ──────────────────────────────────────────────────────────────────────────────
def write_kaggle_metadata(out_dir: str, kaggle_username: str, dataset_slug: str,
                          season_name: str, row_counts: dict):
    updated = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    total_rows = sum(row_counts.values())

    meta = {
        "title": f"Trendyol Süper Lig {season_name} — Full Dataset",
        "id": f"{kaggle_username}/{dataset_slug}",
        "licenses": [{"name": "CC0-1.0"}],
        "description": (
            f"**Trendyol Süper Lig {season_name} — Kapsamlı Veri Seti**\n\n"
            f"Son güncelleme: {updated}\n"
            f"Toplam satır: ~{total_rows:,}\n\n"
            "## Tablolar\n"
            "| Dosya | İçerik | Satır |\n"
            "|---|---|---|\n"
            + "\n".join(
                f"| `{name}.csv` | {_desc(name)} | {cnt:,} |"
                for name, cnt in row_counts.items()
            ) + "\n\n"
            "## Kaynak\n"
            "Sofascore API (sofascore-wrapper 1.1.1)\n\n"
            "## Sütunlar\n"
            "### winner_code\n"
            "- `1` = Ev sahibi galip\n"
            "- `2` = Deplasman galip\n"
            "- `3` = Beraberlik\n\n"
            "### goal_type\n"
            "- `regular` · `penalty` · `ownGoal` · `freekick`\n\n"
            "### card_type\n"
            "- `yellow` · `yellowRed` · `red`\n"
        ),
        "keywords": [
            "soccer", "football", "turkey", "super-lig",
            "match-data", "player-stats", "transfers",
        ],
        "resources": [
            {"path": f"{name}.csv", "description": _desc(name)}
            for name in row_counts
        ],
    }

    path = os.path.join(out_dir, "dataset-metadata.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    log.info(f"  ✓ dataset-metadata.json")
    return path


def _desc(name: str) -> str:
    return {
        "standings":       "Puan durumu",
        "teams":           "Takım profilleri",
        "team_stats":      "Takım sezon istatistikleri",
        "matches":         "Maç sonuçları (tüm haftalar)",
        "goals":           "Goller (atan, asist, dakika, tür)",
        "cards":           "Sarı/kırmızı kartlar",
        "substitutions":   "Oyuncu değişiklikleri",
        "match_stats":     "Maç başı detaylı istatistikler",
        "player_profiles": "Oyuncu profilleri (piyasa değeri, uyruk...)",
        "player_stats":    "Oyuncu sezon istatistikleri",
    }.get(name, name)


# ──────────────────────────────────────────────────────────────────────────────
# Run log
# ──────────────────────────────────────────────────────────────────────────────
def write_run_log(log_path: str, season: dict, counts: dict, status: str, error: str = ""):
    import json
    entry = {
        "ts":      datetime.datetime.utcnow().isoformat(),
        "season":  season.get("name"),
        "status":  status,
        "counts":  counts,
        "error":   error,
    }
    logs = []
    if os.path.exists(log_path):
        with open(log_path) as f:
            try:
                logs = json.load(f)
            except Exception:
                logs = []
    logs.append(entry)
    with open(log_path, "w") as f:
        json.dump(logs, f, indent=2)