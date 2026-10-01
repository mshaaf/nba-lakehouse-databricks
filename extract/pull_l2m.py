"""Pull NBA Last Two Minute (L2M) officiating reports from official.nba.com.

The league grades every call and non-call in the last two minutes of close
games (CC/CNC correct, IC/INC incorrect) and records which team an error
favored. JSON reports exist from 2019-01-01 on; other games return an S3
AccessDenied XML (no report). Every game asked about gets one line, so Bronze
shows exactly what was requested and reruns skip games already checked.
"""
import glob
import json
import time
from pathlib import Path

import requests

from pull_game_logs import OUT, retry

FIRST_REPORT_DATE = "2019-01-01"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Referer": "https://official.nba.com/",
}


def fetch(game_id):
    r = requests.get(f"https://official.nba.com/l2m/json/{game_id}.json", headers=HEADERS, timeout=30)
    if r.ok:
        return {"gameId": game_id, "has_report": True, **r.json()}
    # S3 AccessDenied = no report for this game. Anything else (e.g. an Akamai
    # HTML block page) is a real failure and must not be recorded as "no report".
    if r.status_code == 403 and "AccessDenied" in r.text:
        return {"gameId": game_id, "has_report": False}
    r.raise_for_status()
    raise RuntimeError(f"{game_id}: unexpected {r.status_code}")


def main():
    for log_file in sorted(glob.glob(str(OUT / "team_game_logs" / "*.jsonl"))):
        games = {json.loads(line)["GAME_ID"]: json.loads(line)["GAME_DATE"] for line in open(log_file)}
        game_ids = sorted(g for g, d in games.items() if d >= FIRST_REPORT_DATE)
        if not game_ids:
            continue
        out = OUT / "l2m_reports" / Path(log_file).name
        out.parent.mkdir(parents=True, exist_ok=True)
        done = {json.loads(line)["gameId"] for line in open(out)} if out.exists() else set()
        todo = [g for g in game_ids if g not in done]
        print(f"{out.name}: {len(done)} done, {len(todo)} to check", flush=True)

        with open(out, "a") as f:
            for i, game_id in enumerate(todo, 1):
                f.write(json.dumps(retry(lambda: fetch(game_id))) + "\n")
                f.flush()
                if i % 250 == 0:
                    print(f"  {i}/{len(todo)}", flush=True)
                time.sleep(0.5)


if __name__ == "__main__":
    main()
