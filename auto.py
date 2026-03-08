import argparse
import asyncio
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from sofascore_wrapper.api import SofascoreAPI

import collector as col
import uploader as upl
import writer as wrt

load_dotenv()

LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
TOP_LEAGUES = {
    "Premier League": 17,
    "LaLiga": 8,
    "Serie A": 23,
    "Bundesliga": 35,
    "Ligue 1": 34,
    "Eredivisie": 37,
    "Liga Portugal": 238,
    "Super Lig": 52,
}


def configure_logging(level: str) -> None:
    logging.basicConfig(level=level.upper(), format=LOG_FORMAT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect player profiles and league statistics from selected "
            "SofaScore leagues and optionally upload to Kaggle."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="data/top_10",
        help="Directory for generated CSV files and metadata.",
    )
    parser.add_argument(
        "--league-delay",
        type=float,
        default=3.0,
        help="Delay (in seconds) between league-level requests.",
    )
    parser.add_argument(
        "--max-leagues",
        type=int,
        default=0,
        help="Process only the first N leagues (0 = all leagues).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip Kaggle upload even when credentials are configured.",
    )
    parser.add_argument(
        "--new-dataset",
        action="store_true",
        help="Create a new Kaggle dataset instead of creating a new version.",
    )
    parser.add_argument(
        "--version-notes",
        default="",
        help="Custom Kaggle version notes.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def build_version_notes(custom_notes: str) -> str:
    if custom_notes.strip():
        return custom_notes.strip()
    date_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"Automated dataset update ({date_utc})"


async def run_pipeline(args: argparse.Namespace) -> bool:
    log = logging.getLogger("auto")
    all_profiles, all_stats = [], []
    kaggle_username = os.getenv("KAGGLE_USERNAME")
    kaggle_dataset_slug = os.getenv("KAGGLE_DATASET_SLUG")

    selected_leagues = list(TOP_LEAGUES.items())
    if args.max_leagues > 0:
        selected_leagues = selected_leagues[: args.max_leagues]

    log.info("Pipeline started for %d leagues.", len(selected_leagues))
    api = SofascoreAPI()

    try:
        for league_name, league_id in selected_leagues:
            log.info("Collecting league: %s (id=%s)", league_name, league_id)
            try:
                season = await col.resolve_season(api, league_id)
                season_id = season.get("id") if isinstance(season, dict) else None
                if not season_id:
                    log.warning("Skipping %s because no active season was resolved.", league_name)
                    continue

                profiles, stats = await col.collect_players_for_league(
                    api=api,
                    league_id=league_id,
                    season_id=season_id,
                    league_name=league_name,
                )
            except Exception as exc:
                log.exception("League failed: %s (id=%s): %s", league_name, league_id, exc)
                continue

            all_profiles.extend(profiles)
            all_stats.extend(stats)
            log.info(
                "Completed league: %s | profiles=%d | stats=%d",
                league_name,
                len(profiles),
                len(stats),
            )

            if args.league_delay > 0:
                await asyncio.sleep(args.league_delay)
    finally:
        await api.close()

    if not all_profiles and not all_stats:
        log.error("No data was collected from any league. Aborting run.")
        return False

    os.makedirs(args.output_dir, exist_ok=True)
    wrt.write_csv(
        os.path.join(args.output_dir, "all_player_profiles.csv"),
        all_profiles,
        wrt.SCHEMAS["all_player_profiles"],
    )
    wrt.write_csv(
        os.path.join(args.output_dir, "all_player_stats.csv"),
        all_stats,
        wrt.SCHEMAS["all_player_stats"],
    )

    has_dataset_identity = bool(kaggle_username and kaggle_dataset_slug)
    if has_dataset_identity:
        wrt.write_kaggle_metadata(
            out_dir=args.output_dir,
            kaggle_username=kaggle_username,
            dataset_slug=kaggle_dataset_slug,
            row_counts={
                "all_player_profiles": len(all_profiles),
                "all_player_stats": len(all_stats),
            },
            league_names=[name for name, _ in selected_leagues],
        )
    else:
        log.warning(
            "KAGGLE_USERNAME or KAGGLE_DATASET_SLUG is missing; metadata and upload are skipped."
        )

    if args.no_upload:
        log.info("Upload skipped by --no-upload flag.")
        return True

    if not has_dataset_identity:
        log.info("Upload skipped because Kaggle dataset identity is not configured.")
        return True

    upload_ok = upl.upload_dataset(
        dataset_dir=args.output_dir,
        username=kaggle_username,
        slug=kaggle_dataset_slug,
        version_notes=build_version_notes(args.version_notes),
        new_dataset=args.new_dataset,
    )
    if not upload_ok:
        log.error("Kaggle upload failed.")
        return False

    log.info("Pipeline finished successfully.")
    return True


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    success = asyncio.run(run_pipeline(args))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
