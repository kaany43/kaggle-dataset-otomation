"""
auto.py — Trendyol Süper Lig Otonom Dataset Sistemi
════════════════════════════════════════════════════

Her çalıştığında:
  1. Sofascore'dan güncel veri çeker
  2. CSV'leri ./data/<sezon>/ klasörüne yazar
  3. Kaggle'a yükler (isteğe bağlı)
  4. Çalışma logunu kaydeder

Kullanım:
  python auto.py                         # mevcut sezon, Kaggle'a YÜKLEMEz
  python auto.py --upload                # mevcut sezon + Kaggle yükle
  python auto.py --season 2024           # 2024 sezonu
  python auto.py --season 61627          # sezon ID ile
  python auto.py --modules matches events # sadece bu modüller
  python auto.py --upload --new          # ilk kez yükleme (yeni dataset)

Otomatik çalıştırma:
  Windows Task Scheduler:
    Tetikleyici : Günlük 03:00
    Eylem       : python C:\\proje\\auto.py --upload

  Linux cron:
    0 3 * * * cd /proje && .venv/bin/python auto.py --upload >> logs/cron.log 2>&1
"""

import sys
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import json
import logging
import argparse
import datetime
from dotenv import load_dotenv, find_dotenv

# ─── .env yükle (varsa) ──────────────────────────────────────────────────────
dotenv_path = find_dotenv(usecwd=True)
if dotenv_path:
    load_dotenv(dotenv_path)

# ─── Loglama ─────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", f"run_{datetime.datetime.now():%Y%m%d_%H%M%S}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger("auto")

# ─── Yerel modüller ──────────────────────────────────────────────────────────
from sofascore_wrapper.api import SofascoreAPI
import collector as col
import writer   as wrt
import uploader as upl

# ─── Ayarlar (.env veya env var) ─────────────────────────────────────────────
KAGGLE_USERNAME  = os.getenv("KAGGLE_USERNAME", "")
KAGGLE_SLUG      = os.getenv("KAGGLE_DATASET_SLUG", "superlig-full-dataset")
DATA_ROOT        = os.getenv("DATA_ROOT", "./data")
RUN_LOG_PATH     = os.path.join("logs", "run_history.json")

ALL_MODULES = ["standings", "teams", "matches", "events", "match_stats", "players"]


# ══════════════════════════════════════════════════════════════════════════════
# Ana pipeline
# ══════════════════════════════════════════════════════════════════════════════
async def run_pipeline(
    season_arg: str | None,
    modules: list[str],
    do_upload: bool,
    new_dataset: bool,
):
    start = datetime.datetime.now()
    log.info("=" * 60)
    log.info(f"Pipeline başladı  |  modüller: {modules}")

    api = SofascoreAPI()
    counts = {}
    season = {}

    try:
        # ── Sezon seç ────────────────────────────────────────────────────────
        season = await col.resolve_season(api, int(season_arg) if season_arg and season_arg.isdigit() else None)

        # Yıl bazlı seçim (örn: "2024")
        if season_arg and not season_arg.isdigit():
            all_seasons = await col.get_all_seasons(api)
            matches = [s for s in all_seasons if season_arg in s.get("year", "")]
            if not matches:
                raise ValueError(f"'{season_arg}' yılı bulunamadı")
            season = matches[0]

        sid      = season["id"]
        yr       = season.get("year", str(sid)).replace("/", "-")
        out_dir  = os.path.join(DATA_ROOT, yr)
        os.makedirs(out_dir, exist_ok=True)

        log.info(f"Sezon: {season.get('name')}  (id={sid})")
        log.info(f"Çıktı: {out_dir}")

        # ── Her zaman standings çek (takım ID listesi lazım) ─────────────────
        standings_rows = await col.collect_standings(api, sid)
        counts["standings"] = wrt.write_csv(
            os.path.join(out_dir, "standings.csv"),
            standings_rows, wrt.SCHEMAS["standings"],
        )
        team_ids = [r["team_id"] for r in standings_rows if r.get("team_id")]

        # ── Seçili modülleri çalıştır ────────────────────────────────────────
        played_ids = []

        if "matches" in modules or "events" in modules or "match_stats" in modules:
            match_rows, played_ids = await col.collect_matches(api, sid)
            counts["matches"] = wrt.write_csv(
                os.path.join(out_dir, "matches.csv"),
                match_rows, wrt.SCHEMAS["matches"],
            )

        if "teams" in modules:
            profiles, team_stats = await col.collect_teams(api, sid, team_ids)
            counts["teams"]      = wrt.write_csv(os.path.join(out_dir, "teams.csv"),      profiles,   wrt.SCHEMAS["teams"])
            counts["team_stats"] = wrt.write_csv(os.path.join(out_dir, "team_stats.csv"), team_stats, wrt.SCHEMAS["team_stats"])

        if "events" in modules and played_ids:
            goals, cards, subs = await col.collect_events(api, played_ids)
            counts["goals"]         = wrt.write_csv(os.path.join(out_dir, "goals.csv"),         goals, wrt.SCHEMAS["goals"])
            counts["cards"]         = wrt.write_csv(os.path.join(out_dir, "cards.csv"),         cards, wrt.SCHEMAS["cards"])
            counts["substitutions"] = wrt.write_csv(os.path.join(out_dir, "substitutions.csv"), subs,  wrt.SCHEMAS["substitutions"])

        if "match_stats" in modules and played_ids:
            ms_rows = await col.collect_match_stats(api, played_ids)
            counts["match_stats"] = wrt.write_csv(os.path.join(out_dir, "match_stats.csv"), ms_rows)

        if "players" in modules:
            p_profiles, p_stats = await col.collect_players(api, sid, team_ids)
            counts["player_profiles"] = wrt.write_csv(os.path.join(out_dir, "player_profiles.csv"), p_profiles, wrt.SCHEMAS["player_profiles"])
            counts["player_stats"]    = wrt.write_csv(os.path.join(out_dir, "player_stats.csv"),    p_stats,    wrt.SCHEMAS["player_stats"])

        # ── Kaggle metadata ──────────────────────────────────────────────────
        wrt.write_kaggle_metadata(
            out_dir,
            kaggle_username=KAGGLE_USERNAME or "your_username",
            dataset_slug=KAGGLE_SLUG,
            season_name=season.get("name", yr),
            row_counts=counts,
        )

        # ── Özet ─────────────────────────────────────────────────────────────
        elapsed = (datetime.datetime.now() - start).seconds
        total   = sum(counts.values())
        log.info(f"\n{'─'*50}")
        log.info(f"✅ Tamamlandı  |  {total:,} satır  |  {elapsed}s")
        for name, cnt in counts.items():
            log.info(f"   {name:<22} {cnt:>7,} satır")
        log.info(f"{'─'*50}")

        # ── Kaggle yükleme ────────────────────────────────────────────────────
        upload_ok = False
        if do_upload:
            if not KAGGLE_USERNAME:
                log.error("KAGGLE_USERNAME tanımlı değil! .env dosyasına ekleyin.")
            else:
                version_notes = (
                    f"Auto-update {datetime.datetime.utcnow():%Y-%m-%d %H:%M UTC} "
                    f"— {season.get('name')} — {total:,} rows"
                )
                # İlk kez mi yoksa güncelleme mi?
                exists = upl.check_dataset_exists(KAGGLE_USERNAME, KAGGLE_SLUG)
                upload_ok = upl.upload_dataset(
                    dataset_dir=out_dir,
                    username=KAGGLE_USERNAME,
                    slug=KAGGLE_SLUG,
                    version_notes=version_notes,
                    new_dataset=(new_dataset or not exists),
                )

        # ── Log kaydet ────────────────────────────────────────────────────────
        wrt.write_run_log(RUN_LOG_PATH, season, counts, "success" if not do_upload or upload_ok else "upload_failed")
        return True

    except Exception as ex:
        log.exception(f"Pipeline hatası: {ex}")
        wrt.write_run_log(RUN_LOG_PATH, season, counts, "error", str(ex))
        return False

    finally:
        await api.close()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="Trendyol Süper Lig Otonom Dataset Sistemi",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--season",
        help="Sezon yılı (2024) veya ID (61627). Boş = mevcut sezon.",
    )
    parser.add_argument(
        "--modules", nargs="+",
        choices=ALL_MODULES,
        default=ALL_MODULES,
        help="Çalıştırılacak modüller (varsayılan: hepsi)",
    )
    parser.add_argument(
        "--upload", action="store_true",
        help="Bitince Kaggle'a yükle",
    )
    parser.add_argument(
        "--new", action="store_true",
        help="Kaggle'da yeni dataset oluştur (güncelleme yerine)",
    )
    parser.add_argument(
        "--list-seasons", action="store_true",
        help="Mevcut sezonları listele ve çık",
    )
    args = parser.parse_args()

    # ── Sezon listesi ─────────────────────────────────────────────────────────
    if args.list_seasons:
        async def _list():
            api = SofascoreAPI()
            try:
                seasons = await col.get_all_seasons(api)
                print(f"\n{'ID':>8}  {'Yıl':<10}  Ad")
                print("-" * 40)
                for s in seasons:
                    print(f"{s['id']:>8}  {s.get('year','?'):<10}  {s.get('name','?')}")
            finally:
                await api.close()
        asyncio.run(_list())
        return

    # ── Pipeline ─────────────────────────────────────────────────────────────
    ok = asyncio.run(run_pipeline(
        season_arg=args.season,
        modules=args.modules,
        do_upload=args.upload,
        new_dataset=args.new,
    ))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()