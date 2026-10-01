# Databricks notebook source
# Silver: typed, deduplicated tables, upserted into Delta with MERGE.
#   team_games      one row per team per game (box score totals)
#   game_info       one row per game (home/away ids, neutral site, crowd, arena)
#   game_officials  one row per referee per game
# Bronze is append-only, so a re-pulled season file shows up there twice; the
# latest file version of each row wins. Data quality checks run on the source
# before the MERGE, so bad data fails the job instead of landing.
from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

BRONZE, SILVER = "workspace.nba_bronze", "workspace.nba_silver"
STAT_COLS = ["MIN", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "OREB", "DREB", "REB",
             "AST", "STL", "BLK", "TOV", "PF", "PTS", "PLUS_MINUS"]


def latest(table, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_file_modified_at").desc(), F.col("_ingested_at").desc())
    return spark.table(table).withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")


team_games = latest(f"{BRONZE}.team_game_logs", ["GAME_ID", "TEAM_ID"]).select(
    F.col("GAME_ID").alias("game_id"),
    F.to_date("GAME_DATE").alias("game_date"),
    F.substring("SEASON_ID", 2, 4).cast("int").alias("season_start_year"),
    # SEASON_ID prefix: 2 = regular season, 4 = playoffs. Anything else fails the checks.
    F.when(F.col("SEASON_ID").startswith("2"), "Regular Season")
    .when(F.col("SEASON_ID").startswith("4"), "Playoffs")
    .alias("season_type"),
    F.col("TEAM_ID").cast("bigint").alias("team_id"),
    F.col("TEAM_ABBREVIATION").alias("team_abbr"),
    # "BOS vs. NYK" = home, "BOS @ NYK" = away. Neutral-site games list both teams with "@".
    F.col("MATCHUP").contains(" vs. ").alias("is_home"),
    (F.col("WL") == "W").alias("won"),
    *[F.col(c).cast("int").alias(c.lower()) for c in STAT_COLS],
    F.col("_file_modified_at").alias("source_version"),
)

summaries = latest(f"{BRONZE}.game_summaries", ["gameId"])
# Games the API has no summary for (empty response at the source). Listed by the
# extractor so a coverage gap is explicit instead of silently tolerated.
if spark.catalog.tableExists(f"{BRONZE}.game_summaries_missing"):
    missing_summaries = latest(f"{BRONZE}.game_summaries_missing", ["gameId"]).select(F.col("gameId").alias("game_id"))
else:
    missing_summaries = spark.createDataFrame([], "game_id string")
game_info = summaries.select(
    F.col("gameId").alias("game_id"),
    F.col("homeTeamId").cast("bigint").alias("home_team_id"),
    F.col("awayTeamId").cast("bigint").alias("away_team_id"),
    (F.col("isNeutral") == "true").alias("is_neutral"),
    # 0 can mean "not recorded" (e.g. the 2016-17 opener) as well as an empty arena
    F.col("attendance").cast("int").alias("attendance"),
    (F.col("sellout").cast("int") == 1).alias("sellout"),
    # Bronze keeps nested JSON objects as strings (inferColumnTypes off), so parse here
    F.get_json_object("arena", "$.arenaName").alias("arena_name"),
    F.get_json_object("arena", "$.arenaCity").alias("arena_city"),
    F.col("_file_modified_at").alias("source_version"),
)
game_officials = summaries.select(
    F.col("gameId").alias("game_id"),
    F.explode(F.from_json("officials", "array<struct<personId:bigint, name:string>>")).alias("o"),
    F.col("_file_modified_at").alias("source_version"),
).select(
    "game_id",
    F.col("o.personId").alias("official_id"),
    F.col("o.name").alias("official_name"),
    "source_version",
)

# COMMAND ----------

# Data quality checks: every count must be 0.
games = team_games.groupBy("game_id").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("won").cast("int")).alias("winners"),
    F.sum("plus_minus").alias("plus_minus_sum"),
    F.sum(F.col("is_home").cast("int")).alias("home_teams"),
)
player_pts = (
    latest(f"{BRONZE}.player_game_logs", ["GAME_ID", "PLAYER_ID"])
    .groupBy(F.col("GAME_ID").alias("game_id"), F.col("TEAM_ID").cast("bigint").alias("team_id"))
    .agg(F.sum(F.col("PTS").cast("int")).alias("player_pts"))
)
checks = {
    "null keys or unknown season type": team_games.filter(
        "game_id IS NULL OR team_id IS NULL OR game_date IS NULL OR season_type IS NULL"
    ).count(),
    "games without exactly 2 team rows": games.filter("rows != 2").count(),
    "games without exactly 1 winner": games.filter("winners != 1").count(),
    "games where plus_minus does not net to 0": games.filter("plus_minus_sum != 0").count(),
    "games with 2 home teams": games.filter("home_teams > 1").count(),
    "team pts != sum of player pts": team_games.join(player_pts, ["game_id", "team_id"], "left")
    .filter("player_pts IS NULL OR pts != player_pts")
    .count(),
    "games with no summary and not listed as missing at the source": games.join(game_info, "game_id", "left_anti")
    .join(missing_summaries, "game_id", "left_anti")
    .count(),
    "summaries with no team log game": game_info.join(games, "game_id", "left_anti").count(),
    "summary home team != team log home team": team_games.filter("is_home")
    .join(game_info, "game_id")
    .filter("team_id != home_team_id")
    .count(),
    "games with 0 or more than 4 officials": game_officials.groupBy("game_id").count().filter("count > 4").count()
    + game_info.join(game_officials, "game_id", "left_anti").count(),
}
for name, bad in checks.items():
    print(f"{'OK  ' if bad == 0 else 'FAIL'} {name}: {bad}")
failed = {name: bad for name, bad in checks.items() if bad}
assert not failed, f"data quality checks failed: {failed}"
print("neutral-site games (no home team listed):", games.filter("home_teams = 0").count())
print("games flagged is_neutral:", game_info.filter("is_neutral").count())
print("games documented as having no summary at the source:", missing_summaries.join(game_info, "game_id", "left_anti").count())
print("official counts per game:", {r["count"]: r["n"] for r in game_officials.groupBy("game_id").count().groupBy("count").agg(F.count("*").alias("n")).collect()})

# COMMAND ----------


def upsert(df, table, keys):
    # ponytail: re-merges all of Bronze each run (~40k rows total); switch the
    # source to rows ingested since the last run if Bronze grows past millions.
    target = f"{SILVER}.{table}"
    df.limit(0).write.mode("ignore").saveAsTable(target)
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(df.alias("s"), " AND ".join(f"t.{k} = s.{k}" for k in keys))
        .whenMatchedUpdateAll(condition="s.source_version > t.source_version")
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(table, spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").select("operationMetrics").first()[0])


upsert(team_games, "team_games", ["game_id", "team_id"])
upsert(game_info, "game_info", ["game_id"])
upsert(game_officials, "game_officials", ["game_id", "official_id"])
