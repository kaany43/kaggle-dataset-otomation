import csv
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

log = logging.getLogger("writer")

SCHEMAS = {
    "all_player_profiles": [
        "player_id",
        "name",
        "league",
        "position",
        "market_value",
    ],
    "all_player_stats": [
        "player_id",
        "league",
        "appearances",
        "matches_started",
        "minutes_played",
        "goals",
        "assists",
        "expected_goals",
        "expected_assists",
        "rating",
        "total_shots",
        "shots_on_target",
        "yellow_cards",
        "red_cards",
        "tackles",
        "interceptions",
        "saves",
    ],
}


def write_csv(path: str, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> int:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not rows and not fieldnames:
        log.warning("Skipping empty dataset without schema: %s", path)
        return 0

    if fieldnames is None:
        fieldnames = list(dict.fromkeys(key for row in rows for key in row.keys()))

    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            log.warning("Wrote header-only CSV because dataset is empty: %s", path)

    log.info("Wrote %s with %d rows.", os.path.basename(path), len(rows))
    return len(rows)


def build_dataset_description(
    updated_date: str,
    total_profiles: int,
    total_stats: int,
    league_names: Iterable[str],
) -> str:
    leagues = ", ".join(league_names)
    return (
        "# European Top League Player Dataset\n\n"
        "This dataset contains player profile and season-level performance metrics "
        "for major European football leagues.\n\n"
        f"Last updated (UTC): {updated_date}\n"
        f"Player profiles: {total_profiles:,}\n"
        f"Player stat rows: {total_stats:,}\n\n"
        "## Files\n"
        "- `all_player_profiles.csv`: Player profile attributes such as name, league, "
        "position, and market value.\n"
        "- `all_player_stats.csv`: Season metrics including goals, assists, xG, xA, "
        "rating, and defensive stats.\n\n"
        f"## Covered Leagues\n{leagues}\n\n"
        "Source: SofaScore public endpoints via `sofascore-wrapper`."
    )


def write_kaggle_metadata(
    out_dir: str,
    kaggle_username: str,
    dataset_slug: str,
    row_counts: dict[str, int],
    league_names: Iterable[str],
) -> str:
    if not kaggle_username or not dataset_slug:
        raise ValueError("kaggle_username and dataset_slug are required for metadata generation.")

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_profiles = row_counts.get("all_player_profiles", 0)
    total_stats = row_counts.get("all_player_stats", 0)

    metadata = {
        "title": "European Top League Player Stats",
        "id": f"{kaggle_username}/{dataset_slug}",
        "licenses": [{"name": "CC0-1.0"}],
        "description": build_dataset_description(
            updated_date=updated,
            total_profiles=total_profiles,
            total_stats=total_stats,
            league_names=league_names,
        ),
        "keywords": [
            "football",
            "soccer",
            "player-stats",
            "europe",
            "sports",
            "data-science",
        ],
        "resources": [
            {
                "path": "all_player_profiles.csv",
                "description": "Player profile records.",
            },
            {
                "path": "all_player_stats.csv",
                "description": "Season-level player statistics.",
            },
        ],
    }

    path = os.path.join(out_dir, "dataset-metadata.json")
    with open(path, "w", encoding="utf-8") as file:
        json.dump(metadata, file, ensure_ascii=False, indent=2)

    log.info("Updated dataset metadata at %s.", path)
    return path
