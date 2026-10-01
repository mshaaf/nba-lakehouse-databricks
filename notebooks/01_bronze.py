# Databricks notebook source
# Bronze: ingest landed JSON-lines files into Delta with Auto Loader.
# Auto Loader's checkpoint remembers which files were processed, so a rerun with
# no new files is a no-op. allowOverwrites re-ingests a season file when the
# extractor re-pulls it (box score corrections); Silver keeps the latest version.
# Columns stay as raw strings (inferColumnTypes off); Silver casts them.
from pyspark.sql import functions as F

VOLUME = "/Volumes/workspace/nba_bronze/landing"

for dataset in ["team_game_logs", "player_game_logs", "game_summaries"]:
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{VOLUME}/_autoloader/schemas/{dataset}")
        .option("cloudFiles.allowOverwrites", "true")
        .load(f"{VOLUME}/{dataset}/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .writeStream.option("checkpointLocation", f"{VOLUME}/_autoloader/checkpoints/{dataset}")
        .trigger(availableNow=True)
        .toTable(f"workspace.nba_bronze.{dataset}")
        .awaitTermination()
    )
