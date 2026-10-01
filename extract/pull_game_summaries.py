"""Pull per-game summaries (attendance, sellout, neutral site, arena, officials).

LeagueGameLog has no attendance, so this calls BoxScoreSummaryV3 once per game
(~12.8k games for 10 seasons). Game IDs come from the landed team_game_logs
files, so run pull_game_logs.py first.

Resumable: each game is appended to its season file as soon as it is fetched,
and games already in the file are skipped on rerun.
"""
import glob
import json
import time
from pathlib import Path

from nba_api.stats.library.http import NBAStatsHTTP

from pull_game_logs import OUT, retry

# ~17KB of the ~24KB payload; not needed for any planned analysis
DROP = {"broadcasters", "lastFiveMeetings", "pregameCharts", "postgameCharts"}


def fetch(game_id):
    # Raw call, not the BoxScoreSummaryV3 class: for some games the API returns an
    # empty shell (gameId null, arena null) and nba_api's parser crashes on it.
    return NBAStatsHTTP().send_api_request(
        endpoint="boxscoresummaryv3", parameters={"GameID": game_id}, timeout=60
    ).get_dict()["boxScoreSummary"]


def main():
    for log_file in sorted(glob.glob(str(OUT / "team_game_logs" / "*.jsonl"))):
        game_ids = sorted({json.loads(line)["GAME_ID"] for line in open(log_file)})
        out = OUT / "game_summaries" / Path(log_file).name
        out.parent.mkdir(parents=True, exist_ok=True)
        done = {json.loads(line)["gameId"] for line in open(out)} if out.exists() else set()
        todo = [g for g in game_ids if g not in done]
        print(f"{out.name}: {len(done)} done, {len(todo)} to pull", flush=True)

        missing = []  # games the API has no summary for; retried on every run
        with open(out, "a") as f:
            for i, game_id in enumerate(todo, 1):
                bs = retry(lambda: fetch(game_id))
                if bs.get("gameId") is None:
                    missing.append(game_id)
                else:
                    f.write(json.dumps({k: v for k, v in bs.items() if k not in DROP}) + "\n")
                    f.flush()
                if i % 250 == 0:
                    print(f"  {i}/{len(todo)}", flush=True)
                time.sleep(0.6)  # stats.nba.com rate limits are undocumented; stay polite

        # Silver requires every game without a summary to be listed here, so a gap is explicit.
        miss_path = OUT / "game_summaries_missing" / Path(log_file).name
        if missing:
            miss_path.parent.mkdir(parents=True, exist_ok=True)
            miss_path.write_text("".join(json.dumps({"gameId": g}) + "\n" for g in missing))
            print(f"  {len(missing)} games have no summary at the source", flush=True)
        else:
            miss_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
