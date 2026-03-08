import asyncio
import logging
import re
from typing import Any

from sofascore_wrapper.api import SofascoreAPI
from sofascore_wrapper.league import League
from sofascore_wrapper.player import Player
from sofascore_wrapper.team import Team

log = logging.getLogger("collector")
RATE_DELAY = 0.6
PLAYER_STAT_KEYS = [
    "appearances",
    "matchesStarted",
    "minutesPlayed",
    "goals",
    "assists",
    "expectedGoals",
    "expectedAssists",
    "rating",
    "totalShots",
    "shotsOnTarget",
    "yellowCards",
    "redCards",
    "tackles",
    "interceptions",
    "saves",
]


def to_snake_case(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


async def resolve_season(api: SofascoreAPI, league_id: int) -> dict[str, Any]:
    return await League(api, league_id).current_season()


async def get_team_ids(api: SofascoreAPI, league_id: int, season_id: int) -> list[int]:
    data = await League(api, league_id).standings(season_id)
    team_ids: list[int] = []

    for group in data.get("standings", []):
        for row in group.get("rows", []):
            team_id = row.get("team", {}).get("id")
            if team_id:
                team_ids.append(team_id)

    return team_ids


async def collect_players_for_league(
    api: SofascoreAPI,
    league_id: int,
    season_id: int,
    league_name: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    profiles: list[dict[str, Any]] = []
    stats: list[dict[str, Any]] = []
    seen_player_ids: set[int] = set()
    team_ids = await get_team_ids(api, league_id, season_id)

    for team_id in team_ids:
        try:
            squad_data = await Team(api, team_id).squad()
        except Exception as exc:
            log.warning(
                "Failed to fetch squad for league=%s team_id=%s: %s",
                league_name,
                team_id,
                exc,
            )
            continue

        for entry in squad_data.get("players", []):
            player = entry.get("player", {})
            player_id = player.get("id")
            if not player_id or player_id in seen_player_ids:
                continue

            seen_player_ids.add(player_id)
            profiles.append(
                {
                    "player_id": player_id,
                    "name": player.get("name"),
                    "league": league_name,
                    "position": player.get("position"),
                    "market_value": (player.get("proposedMarketValueRaw") or {}).get("value"),
                }
            )

            try:
                stats_response = await Player(api, player_id).league_stats(league_id, season_id)
            except Exception as exc:
                message = str(exc)
                if "404" in message:
                    log.debug(
                        "No season stats for league=%s player_id=%s: %s",
                        league_name,
                        player_id,
                        message,
                    )
                else:
                    log.warning(
                        "Failed to fetch stats for league=%s player_id=%s: %s",
                        league_name,
                        player_id,
                        message,
                    )
                await asyncio.sleep(RATE_DELAY)
                continue

            statistics = stats_response.get("statistics", {})
            if statistics:
                row: dict[str, Any] = {"player_id": player_id, "league": league_name}
                for key in PLAYER_STAT_KEYS:
                    row[to_snake_case(key)] = statistics.get(key)
                stats.append(row)

            await asyncio.sleep(RATE_DELAY)

    return profiles, stats
