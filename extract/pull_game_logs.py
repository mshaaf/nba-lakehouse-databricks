"""Pull NBA team and player game logs from stats.nba.com into local JSON-lines files.

Runs on a local machine, not in Databricks: stats.nba.com times out from
Databricks serverless (cloud IP block), while the same call answers locally.
One LeagueGameLog call returns a whole season, so 10 seasons is ~40 calls.
"""
import argparse
import time
from pathlib import Path

from nba_api.stats.endpoints import leaguegamelog

OUT = Path(__file__).resolve().parent.parent / "data" / "landing"
SEASON_TYPES = ["Regular Season", "Playoffs"]
LEVELS = {"T": "team_game_logs", "P": "player_game_logs"}


def season_str(start_year):
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def retry(call, retries=4):
    for attempt in range(retries):
        try:
            return call()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 5 * 2**attempt
            print(f"  {type(e).__name__}, retry in {wait}s")
            time.sleep(wait)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from-season", type=int, default=2016, help="start year, 2016 = 2016-17")
    p.add_argument("--to-season", type=int, default=2025, help="start year, 2025 = 2025-26")
    args = p.parse_args()

    for year in range(args.from_season, args.to_season + 1):
        for season_type in SEASON_TYPES:
            for level, dataset in LEVELS.items():
                df = retry(
                    lambda: leaguegamelog.LeagueGameLog(
                        season=season_str(year),
                        season_type_all_star=season_type,
                        player_or_team_abbreviation=level,
                        timeout=60,
                    ).get_data_frames()[0]
                )
                path = OUT / dataset / f"{season_str(year)}_{season_type.replace(' ', '_').lower()}.jsonl"
                path.parent.mkdir(parents=True, exist_ok=True)
                df.to_json(path, orient="records", lines=True)
                print(f"{dataset}/{path.name}: {len(df)} rows")
                time.sleep(1)  # stats.nba.com rate limits are undocumented; stay polite


if __name__ == "__main__":
    main()
