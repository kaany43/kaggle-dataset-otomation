"""
collector.py — Sofascore veri çekme katmanı
Tüm veri toplama mantığı burada. auto.py tarafından çağrılır.
"""

import sys, asyncio, time, logging
from typing import Optional

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from sofascore_wrapper.api    import SofascoreAPI
from sofascore_wrapper.league import League
from sofascore_wrapper.team   import Team
from sofascore_wrapper.match  import Match
from sofascore_wrapper.player import Player

log = logging.getLogger("collector")

LEAGUE_ID  = 52      # Trendyol Süper Lig
RATE_DELAY = 0.4     # istek arası bekleme (saniye)


# ──────────────────────────────────────────────────────────────────────────────
# Yardımcılar
# ──────────────────────────────────────────────────────────────────────────────
def _safe(val, default=None):
    return val if val is not None else default


async def _gather(*coros):
    """return_exceptions=True ile gather; hata logla, None dön."""
    results = await asyncio.gather(*coros, return_exceptions=True)
    return [None if isinstance(r, Exception) else r for r in results]


# ──────────────────────────────────────────────────────────────────────────────
# Sezon
# ──────────────────────────────────────────────────────────────────────────────
async def get_all_seasons(api: SofascoreAPI) -> list[dict]:
    return await League(api, LEAGUE_ID).get_seasons()


async def get_current_season(api: SofascoreAPI) -> dict:
    return await League(api, LEAGUE_ID).current_season()


async def resolve_season(api: SofascoreAPI, season_id: Optional[int]) -> dict:
    if season_id is None:
        return await get_current_season(api)
    seasons = await get_all_seasons(api)
    for s in seasons:
        if s["id"] == season_id:
            return s
    raise ValueError(f"season_id={season_id} bulunamadı")


# ──────────────────────────────────────────────────────────────────────────────
# Puan Durumu → takım ID listesi
# ──────────────────────────────────────────────────────────────────────────────
async def collect_standings(api: SofascoreAPI, season_id: int) -> list[dict]:
    data = await League(api, LEAGUE_ID).standings(season_id)
    rows = []
    for group in data.get("standings", []):
        for row in group.get("rows", []):
            team = row.get("team", {})
            rows.append({
                "season_id":     season_id,
                "position":      row.get("position"),
                "team_id":       team.get("id"),
                "team":          team.get("name"),
                "team_code":     team.get("nameCode"),
                "played":        row.get("matches"),
                "wins":          row.get("wins"),
                "draws":         row.get("draws"),
                "losses":        row.get("losses"),
                "goals_for":     row.get("scoresFor"),
                "goals_against": row.get("scoresAgainst"),
                "goal_diff":     _safe(row.get("scoresFor"), 0) - _safe(row.get("scoresAgainst"), 0),
                "points":        row.get("points"),
            })
    log.info(f"Standings: {len(rows)} takım")
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Takım Profilleri + Sezon İstatistikleri
# ──────────────────────────────────────────────────────────────────────────────
async def collect_teams(api: SofascoreAPI, season_id: int, team_ids: list[int]):
    profiles, stats = [], []

    for tid in team_ids:
        try:
            t = Team(api, tid)
            info_r, stat_r = await _gather(
                t.get_team(),
                t.league_stats(LEAGUE_ID, season_id),
            )

            # — profil —
            if info_r:
                tm  = info_r.get("team", {})
                ven = tm.get("venue", {})
                mgr = tm.get("manager", {})
                frm = info_r.get("pregameForm") or {}
                profiles.append({
                    "team_id":       tm.get("id"),
                    "name":          tm.get("name"),
                    "short_name":    tm.get("shortName"),
                    "name_code":     tm.get("nameCode"),
                    "manager":       mgr.get("name"),
                    "stadium":       ven.get("name"),
                    "stadium_city":  (ven.get("city") or {}).get("name"),
                    "capacity":      ven.get("capacity"),
                    "primary_color": (tm.get("teamColors") or {}).get("primary"),
                    "form_5":        ",".join(frm.get("form", [])),
                    "avg_rating":    frm.get("avgRating"),
                })

            # — sezon istatistikleri —
            if stat_r:
                s = stat_r.get("statistics", {})
                stats.append({
                    "team_id":                        tid,
                    "season_id":                      season_id,
                    "matches":                        s.get("matches"),
                    "goals_scored":                   s.get("goalsScored"),
                    "goals_conceded":                 s.get("goalsConceded"),
                    "assists":                        s.get("assists"),
                    "shots":                          s.get("shots"),
                    "shots_on_target":                s.get("shotsOnTarget"),
                    "big_chances":                    s.get("bigChances"),
                    "big_chances_created":            s.get("bigChancesCreated"),
                    "big_chances_missed":             s.get("bigChancesMissed"),
                    "avg_possession":                 s.get("averageBallPossession"),
                    "total_passes":                   s.get("totalPasses"),
                    "accurate_passes_pct":            s.get("accuratePassesPercentage"),
                    "accurate_long_balls_pct":        s.get("accurateLongBallsPercentage"),
                    "accurate_crosses_pct":           s.get("accurateCrossesPercentage"),
                    "corners":                        s.get("corners"),
                    "tackles":                        s.get("tackles"),
                    "interceptions":                  s.get("interceptions"),
                    "clearances":                     s.get("clearances"),
                    "saves":                          s.get("saves"),
                    "clean_sheets":                   s.get("cleanSheets"),
                    "errors_leading_to_goal":         s.get("errorsLeadingToGoal"),
                    "yellow_cards":                   s.get("yellowCards"),
                    "red_cards":                      s.get("redCards"),
                    "avg_rating":                     s.get("avgRating"),
                    "duels_won_pct":                  s.get("duelsWonPercentage"),
                    "aerial_duels_won_pct":           s.get("aerialDuelsWonPercentage"),
                    "successful_dribbles":            s.get("successfulDribbles"),
                    "offsides":                       s.get("offsides"),
                    "fouls":                          s.get("fouls"),
                })

            time.sleep(RATE_DELAY)

        except Exception as ex:
            log.warning(f"Takım {tid}: {ex}")

    log.info(f"Teams: {len(profiles)} profil, {len(stats)} istatistik")
    return profiles, stats


# ──────────────────────────────────────────────────────────────────────────────
# Maçlar (tüm haftalar)
# ──────────────────────────────────────────────────────────────────────────────
async def collect_matches(api: SofascoreAPI, season_id: int) -> tuple[list, list]:
    """
    Döndürür: (matches_rows, played_match_ids)
    played_match_ids → incidents/stats için kullanılır
    """
    league      = League(api, LEAGUE_ID)
    rounds_data = await league.rounds(season_id)
    all_rounds  = rounds_data.get("rounds", [])
    rows, played_ids = [], []

    for r in all_rounds:
        rn = r.get("round")
        try:
            result = await league.league_fixtures_per_round(season_id, rn)
            events = result if isinstance(result, list) else result.get("events", [])

            for e in events:
                sc     = e.get("status", {}).get("code")
                hs     = e.get("homeScore", {})
                as_    = e.get("awayScore", {})
                ri     = e.get("roundInfo", {})
                rows.append({
                    "match_id":        e.get("id"),
                    "season_id":       season_id,
                    "round":           ri.get("round", rn),
                    "start_ts":        e.get("startTimestamp"),
                    "home_team_id":    e.get("homeTeam", {}).get("id"),
                    "home_team":       e.get("homeTeam", {}).get("name"),
                    "away_team_id":    e.get("awayTeam", {}).get("id"),
                    "away_team":       e.get("awayTeam", {}).get("name"),
                    "home_score":      hs.get("current"),
                    "away_score":      as_.get("current"),
                    "home_ht_score":   hs.get("period1"),
                    "away_ht_score":   as_.get("period1"),
                    "winner_code":     e.get("winnerCode"),  # 1=ev 2=dep 3=ber
                    "status":          e.get("status", {}).get("description"),
                    "status_code":     sc,
                })
                if sc == 100:
                    played_ids.append(e["id"])

            time.sleep(RATE_DELAY)
        except Exception as ex:
            log.warning(f"Hafta {rn}: {ex}")

    log.info(f"Matches: {len(rows)} toplam, {len(played_ids)} oynandı")
    return rows, played_ids


# ──────────────────────────────────────────────────────────────────────────────
# Olaylar: Gol / Kart / Değişiklik
# ──────────────────────────────────────────────────────────────────────────────
async def collect_events(api: SofascoreAPI, match_ids: list[int]):
    goals, cards, subs = [], [], []

    for mid in match_ids:
        try:
            data = await Match(api, mid).incidents()
            for inc in data.get("incidents", []):
                t     = inc.get("incidentType", "")
                p     = inc.get("player") or {}
                minute = inc.get("time")
                at     = inc.get("addedTime")
                home   = inc.get("isHome")

                if t == "goal":
                    a = inc.get("assist1") or {}
                    goals.append({
                        "match_id":   mid,
                        "minute":     minute,
                        "added_time": at,
                        "scorer":     p.get("name"),
                        "scorer_id":  p.get("id"),
                        "assist":     a.get("name"),
                        "assist_id":  a.get("id"),
                        "goal_type":  inc.get("incidentClass", "regular"),
                        "is_home":    home,
                        "home_score": inc.get("homeScore"),
                        "away_score": inc.get("awayScore"),
                    })

                elif t == "card":
                    cards.append({
                        "match_id":  mid,
                        "minute":    minute,
                        "added_time": at,
                        "player":    p.get("name"),
                        "player_id": p.get("id"),
                        "card_type": inc.get("incidentClass", "yellow"),
                        "is_home":   home,
                    })

                elif t == "substitution":
                    pi = inc.get("playerIn")  or {}
                    po = inc.get("playerOut") or {}
                    subs.append({
                        "match_id":      mid,
                        "minute":        minute,
                        "player_in":     pi.get("name"),
                        "player_in_id":  pi.get("id"),
                        "player_out":    po.get("name"),
                        "player_out_id": po.get("id"),
                        "is_home":       home,
                    })

            time.sleep(RATE_DELAY)
        except Exception as ex:
            log.warning(f"Maç {mid} olaylar: {ex}")

    log.info(f"Events: {len(goals)} gol, {len(cards)} kart, {len(subs)} değişiklik")
    return goals, cards, subs


# ──────────────────────────────────────────────────────────────────────────────
# Maç İstatistikleri
# ──────────────────────────────────────────────────────────────────────────────
async def collect_match_stats(api: SofascoreAPI, match_ids: list[int]) -> list[dict]:
    rows = []
    for mid in match_ids:
        try:
            data = await Match(api, mid).stats()
            for period in data.get("statistics", []):
                if period.get("period") != "ALL":
                    continue
                flat = {"match_id": mid}
                for group in period.get("groups", []):
                    for item in group.get("statisticsItems", []):
                        key = item.get("key", "").replace(" ", "_").lower()
                        flat[f"home_{key}"] = item.get("homeValue")
                        flat[f"away_{key}"] = item.get("awayValue")
                rows.append(flat)
            time.sleep(RATE_DELAY)
        except Exception as ex:
            log.warning(f"Maç {mid} stats: {ex}")

    log.info(f"Match stats: {len(rows)} maç")
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Oyuncu Profilleri + Sezon İstatistikleri
# ──────────────────────────────────────────────────────────────────────────────
PLAYER_STAT_KEYS = [
    "appearances", "matchesStarted", "minutesPlayed",
    "goals", "assists", "expectedGoals", "expectedAssists",
    "goalsAssistsSum", "penaltyGoals", "freeKickGoals", "headedGoals",
    "leftFootGoals", "rightFootGoals",
    "totalShots", "shotsOnTarget", "shotsOffTarget",
    "bigChancesCreated", "bigChancesMissed",
    "keyPasses", "totalPasses", "accuratePasses", "accuratePassesPercentage",
    "accurateLongBalls", "accurateLongBallsPercentage",
    "accurateCrosses", "accurateCrossesPercentage",
    "successfulDribbles", "successfulDribblesPercentage",
    "tackles", "tacklesWon", "interceptions", "clearances",
    "totalDuelsWon", "totalDuelsWonPercentage",
    "groundDuelsWon", "groundDuelsWonPercentage",
    "aerialDuelsWon", "aerialDuelsWonPercentage",
    "saves", "cleanSheet", "goalsConceded", "penaltySave",
    "yellowCards", "redCards", "yellowRedCards",
    "fouls", "wasFouled", "offsides", "dispossessed",
    "rating",
]

PLAYER_STAT_RENAME = {k: _snake(k) for k in PLAYER_STAT_KEYS} if False else {}  # placeholder


def _snake(s: str) -> str:
    import re
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()


async def collect_players(
    api: SofascoreAPI,
    season_id: int,
    team_ids: list[int],
) -> tuple[list, list]:
    profiles, stats = [], []
    seen_ids = set()

    for tid in team_ids:
        try:
            squad_data = await Team(api, tid).squad()
            players    = squad_data.get("players", [])

            for entry in players:
                p   = entry.get("player", {})
                pid = p.get("id")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)

                # — profil —
                mv  = (p.get("proposedMarketValueRaw") or {})
                nat = (p.get("country") or {})
                profiles.append({
                    "player_id":        pid,
                    "name":             p.get("name"),
                    "short_name":       p.get("shortName"),
                    "position":         p.get("position"),
                    "jersey_number":    p.get("jerseyNumber"),
                    "height_cm":        p.get("height"),
                    "preferred_foot":   p.get("preferredFoot"),
                    "dob_timestamp":    p.get("dateOfBirthTimestamp"),
                    "nationality":      nat.get("name"),
                    "nationality_iso2": nat.get("alpha2"),
                    "team_id":          tid,
                    "market_value_eur": mv.get("value"),
                    "market_value_cur": mv.get("currency"),
                })

                # — sezon istatistikleri —
                try:
                    sr = await Player(api, pid).league_stats(LEAGUE_ID, season_id)
                    s  = sr.get("statistics", {})
                    if s:
                        row = {
                            "player_id": pid,
                            "team_id":   tid,
                            "season_id": season_id,
                        }
                        for key in PLAYER_STAT_KEYS:
                            row[_snake(key)] = s.get(key)
                        stats.append(row)
                    time.sleep(RATE_DELAY)
                except Exception:
                    pass

            time.sleep(0.5)
            log.debug(f"Takım {tid}: {len(players)} oyuncu")

        except Exception as ex:
            log.warning(f"Takım {tid} kadro: {ex}")

    log.info(f"Players: {len(profiles)} profil, {len(stats)} istatistik")
    return profiles, stats