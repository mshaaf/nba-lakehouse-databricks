# Databricks notebook source
# Gold: "home whistle by ref". For each referee, how much more do home teams
# get to the line (FTA) and how many fewer fouls do they get called against
# them, compared with the average ref, after adjusting for the teams playing?
#
#   game_whistle         one row per game (neutral-site and bubble games excluded)
#   home_edge_by_season  league-wide baseline per season and season type
#   ref_home_whistle     per-referee adjusted effect with bootstrap 95% interval
#
# Sign convention: positive = favors the home team.
#   fta_diff  = home FTA - away FTA
#   foul_diff = away PF  - home PF
#
# Model (ridge regression, closed form, numpy only): per game
#   y = season intercept + sum(ref effects) / 3
#       + home team-season effect - away team-season effect + noise
# Fitted on regular-season games with exactly 3 officials. Playoff crews often
# list a 4th official and the feed's `assignment` field is blank, so who calls
# fouls there is ambiguous; playoffs stay in game_whistle and the baseline table.
# Crews rotate, so each ref works with many partners and teams; that is what
# lets one ref's effect be separated from the crew's and from team style.
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

SILVER, GOLD = "workspace.nba_silver", "workspace.nba_gold"
BUBBLE = ("2020-07-30", "2020-10-11")  # Orlando bubble: no home court, no travel
MIN_GAMES = 200  # leaderboard threshold; the model still fits every ref
N_BOOT = 500
TARGETS = ["fta_diff", "foul_diff"]

# COMMAND ----------

team_games = spark.table(f"{SILVER}.team_games")
game_info = spark.table(f"{SILVER}.game_info").select("game_id", "attendance", "is_neutral")

home = team_games.filter("is_home").select(
    "game_id", "game_date", "season_start_year", "season_type",
    F.col("team_id").alias("home_team_id"), F.col("fta").alias("home_fta"),
    F.col("pf").alias("home_pf"), F.col("won").alias("home_won"),
)
away = team_games.filter(~F.col("is_home")).select(
    "game_id", F.col("team_id").alias("away_team_id"),
    F.col("fta").alias("away_fta"), F.col("pf").alias("away_pf"),
)
# Games where both teams list "@" (no home team) have no row in `home`, so the
# inner join drops them. is_neutral catches any neutral site that still lists one.
all_games = home.join(away, "game_id").join(game_info, "game_id")
whistle = (
    all_games.filter(~F.col("is_neutral"))
    .filter(~F.col("game_date").between(*BUBBLE))
    .withColumn("fta_diff", F.col("home_fta") - F.col("away_fta"))
    .withColumn("foul_diff", F.col("away_pf") - F.col("home_pf"))
)
whistle.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.game_whistle")
whistle = spark.table(f"{GOLD}.game_whistle")  # re-read the persisted table

print("games in Silver:", team_games.select("game_id").distinct().count())
print("games kept for the model:", whistle.count())
print("excluded neutral:", all_games.filter("is_neutral").count(),
      "| excluded bubble:", all_games.filter(F.col("game_date").between(*BUBBLE)).count())

(
    whistle.groupBy("season_start_year", "season_type")
    .agg(
        F.count("*").alias("games"),
        F.round(F.avg("fta_diff"), 3).alias("avg_fta_diff"),
        F.round(F.avg("foul_diff"), 3).alias("avg_foul_diff"),
        F.round(F.avg(F.col("home_won").cast("int")), 4).alias("home_win_pct"),
    )
    .orderBy("season_start_year", "season_type")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.home_edge_by_season")
)

# COMMAND ----------

# Model code (pure numpy/pandas, no Spark).


def build_design(g, crews):
    """g: one row per game; crews: (game_id, official_id) rows for those games.

    Columns: season intercepts | refs | team-seasons.
    Each ref column is 1/crew_size, so a ref's coefficient is the effect of a
    crew made entirely of that ref (comparable to their raw per-game average).
    """
    n = len(g)
    seasons = sorted(g.season_start_year.unique())
    refs = np.sort(crews.official_id.unique())
    team_seasons = sorted(
        set(zip(g.home_team_id, g.season_start_year)) | set(zip(g.away_team_id, g.season_start_year))
    )
    season_idx = {s: i for i, s in enumerate(seasons)}
    ref_idx = {r: i for i, r in enumerate(refs)}
    ts_idx = {t: i for i, t in enumerate(team_seasons)}

    n_s, n_r, n_t = len(seasons), len(refs), len(team_seasons)
    X = np.zeros((n, n_s + n_r + n_t))
    rows = np.arange(n)
    X[rows, g.season_start_year.map(season_idx).to_numpy()] = 1

    pos = pd.Series(rows, index=g.game_id.to_numpy())
    crew_rows = pos.loc[crews.game_id].to_numpy()
    crew_size = crews.groupby("game_id").official_id.transform("size").to_numpy()
    X[crew_rows, n_s + crews.official_id.map(ref_idx).to_numpy()] = 1 / crew_size

    t0 = n_s + n_r
    X[rows, t0 + np.array([ts_idx[(t, s)] for t, s in zip(g.home_team_id, g.season_start_year)])] = 1
    X[rows, t0 + np.array([ts_idx[(t, s)] for t, s in zip(g.away_team_id, g.season_start_year)])] = -1
    return X, {"refs": refs, "ref_cols": slice(n_s, n_s + n_r), "n_s": n_s, "n_r": n_r, "n_t": n_t}


def penalty_vector(meta, lam_ref, lam_team=1.0):
    # Season intercepts are left (almost) unpenalized.
    return np.concatenate([np.full(meta["n_s"], 1e-9), np.full(meta["n_r"], lam_ref), np.full(meta["n_t"], lam_team)])


def fit_ridge(X, Y, penalty, meta, w=None):
    """Weighted ridge. Ref effects are centered on the average ref (weighted by games worked)."""
    Xw = X if w is None else X * w[:, None]
    B = np.linalg.solve(X.T @ Xw + np.diag(penalty), Xw.T @ Y)
    ref = B[meta["ref_cols"]]
    worked = (X[:, meta["ref_cols"]] > 0).astype(float)
    games_worked = worked.sum(0) if w is None else (worked * w[:, None]).sum(0)
    return B, ref - (games_worked @ ref) / games_worked.sum()


def pick_lambda(X, y, meta, grid, k=5, seed=0):
    """K-fold CV over the ref penalty, on held-out squared error."""
    folds = np.random.default_rng(seed).integers(0, k, len(y))
    losses = []
    for lam in grid:
        pen, err = penalty_vector(meta, lam), 0.0
        for f in range(k):
            tr, te = folds != f, folds == f
            B, _ = fit_ridge(X[tr], y[tr], pen, meta)
            err += ((y[te] - X[te] @ B) ** 2).sum()
        losses.append(err / len(y))
    return grid[int(np.argmin(losses))], losses


def bootstrap_ci(X, Y, penalty, meta, n_boot, seed=1):
    rng = np.random.default_rng(seed)
    draws = np.empty((n_boot, meta["n_r"], Y.shape[1]))
    for b in range(n_boot):
        w = rng.multinomial(len(X), np.full(len(X), 1 / len(X))).astype(float)
        draws[b] = fit_ridge(X, Y, penalty, meta, w)[1]
    return np.percentile(draws, 2.5, axis=0), np.percentile(draws, 97.5, axis=0)

# COMMAND ----------

g = whistle.filter("season_type = 'Regular Season'").toPandas()
g[["fta_diff", "foul_diff"]] = g[["fta_diff", "foul_diff"]].astype(float)
officials = spark.table(f"{SILVER}.game_officials").select("game_id", "official_id", "official_name").toPandas()
crews = officials[officials.game_id.isin(g.game_id)]
crew_size = crews.groupby("game_id").official_id.transform("size")
print("regular-season games:", len(g), "| not exactly 3 officials (dropped from the model):", int((crews[crew_size != 3].game_id.nunique())))
crews = crews[crew_size == 3]
# Sorted so cross-validation folds and bootstrap draws do not depend on the
# (non-deterministic) row order of toPandas().
g = g[g.game_id.isin(crews.game_id)].sort_values("game_id").reset_index(drop=True)

X, meta = build_design(g, crews)
Y = g[TARGETS].to_numpy()
games_worked = (X[:, meta["ref_cols"]] > 0).sum(0)
eligible = games_worked >= MIN_GAMES
print(f"design {X.shape}: {meta['n_r']} refs ({eligible.sum()} with {MIN_GAMES}+ games), {meta['n_t']} team-seasons")

# Sanity checks on the data feeding the model.
assert np.isfinite(X).all() and np.isfinite(Y).all(), "non-finite values in model inputs"
assert games_worked.min() >= 1, "ref with no games"
assert len(crews) == 3 * len(g), "every modeled game must have exactly 3 officials"

# COMMAND ----------

# Estimator self-check: plant known ref effects in the real design matrix, add
# noise at the real scale, refit, and require the planted effects back. A broken
# estimator (wrong sign, bad centering, bad design) fails the job here.
lam, losses = pick_lambda(X, Y[:, 0], meta, [1, 3, 10, 30, 100, 300])
print("ref penalty chosen by 5-fold CV:", lam, "| CV mse by lambda:", np.round(losses, 3))

rng = np.random.default_rng(42)
true_beta = np.zeros(X.shape[1])
true_beta[: meta["n_s"]] = rng.normal(2, 1, meta["n_s"])
true_beta[meta["ref_cols"]] = rng.normal(0, 2.5, meta["n_r"])  # strong on purpose: tests the machinery, not power
true_beta[meta["n_s"] + meta["n_r"] :] = rng.normal(0, 2, meta["n_t"])
resid_sd = float(np.std(Y[:, 0] - X @ fit_ridge(X, Y[:, 0], penalty_vector(meta, lam), meta)[0]))
y_syn = X @ true_beta + rng.normal(0, resid_sd, len(X))
_, est_syn = fit_ridge(X, y_syn, penalty_vector(meta, lam), meta)
true_ref = true_beta[meta["ref_cols"]] - np.average(true_beta[meta["ref_cols"]], weights=games_worked)
r = np.corrcoef(est_syn[eligible], true_ref[eligible])[0, 1]
print(f"planted-effect recovery: corr {r:.3f} on {eligible.sum()} refs (residual sd {resid_sd:.2f})")
assert r > 0.7, f"estimator failed to recover planted ref effects (corr {r:.3f})"

# COMMAND ----------

pen = penalty_vector(meta, lam)
_, est = fit_ridge(X, Y, pen, meta)
lo, hi = bootstrap_ci(X, Y, pen, meta, N_BOOT)

# Raw (unadjusted) per-ref average of each target over the games they worked.
worked = (X[:, meta["ref_cols"]] > 0).astype(float)
raw = (worked.T @ Y) / games_worked[:, None]

names = officials.sort_values("game_id").drop_duplicates("official_id", keep="last").set_index("official_id").official_name
out = pd.DataFrame({"official_id": meta["refs"], "official_name": names.loc[meta["refs"]].to_numpy(), "games": games_worked, "eligible": eligible})
for j, t in enumerate(TARGETS):
    out[f"raw_{t}"] = raw[:, j]
    out[f"adj_{t}"] = est[:, j]
    out[f"ci_low_{t}"] = lo[:, j]
    out[f"ci_high_{t}"] = hi[:, j]
    out[f"ci_excludes_zero_{t}"] = (lo[:, j] > 0) | (hi[:, j] < 0)

assert np.isfinite(out.select_dtypes("number").to_numpy()).all(), "non-finite ref estimates"
spark.createDataFrame(out).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.ref_home_whistle")

e = out[out.eligible]
for t in TARGETS:
    n_sig = int(e[f"ci_excludes_zero_{t}"].sum())
    print(f"{t}: {n_sig} of {len(e)} eligible refs have a 95% CI excluding 0 "
          f"(~{0.05 * len(e):.1f} expected by chance alone)")

# COMMAND ----------

# Permutation null: shuffle which crew worked which game within each season.
# Any real ref effect is destroyed; team and season structure is kept. If the
# real data flags clearly more refs than the shuffles do, the spread between
# refs is more than noise. (200 bootstrap draws per fit here, for speed.)
N_PERM, N_BOOT_NULL = 20, 200


def n_flagged(Xm, seed):
    lo, hi = bootstrap_ci(Xm, Y, pen, meta, N_BOOT_NULL, seed=seed)
    return [int(((lo[:, j] > 0) | (hi[:, j] < 0))[eligible].sum()) for j in range(len(TARGETS))]


rc = np.arange(meta["ref_cols"].start, meta["ref_cols"].stop)
rng = np.random.default_rng(11)
rows = [("observed", -1, *n_flagged(X, 1))]
for p in range(N_PERM):
    Xp = X.copy()
    for season in g.season_start_year.unique():
        idx = np.where(g.season_start_year.to_numpy() == season)[0]
        Xp[np.ix_(idx, rc)] = X[np.ix_(rng.permutation(idx), rc)]
    rows.append(("shuffled", p, *n_flagged(Xp, 100 + p)))
null = pd.DataFrame(rows, columns=["kind", "shuffle", "flagged_fta_diff", "flagged_foul_diff"])
spark.createDataFrame(null).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.ref_home_whistle_null")

shuffled = null[null.kind == "shuffled"]
for t in TARGETS:
    obs = int(null.loc[null.kind == "observed", f"flagged_{t}"].iloc[0])
    sh = shuffled[f"flagged_{t}"]
    print(f"{t}: observed {obs} refs flagged | shuffled mean {sh.mean():.1f}, max {sh.max()} | "
          f"shuffles with >= observed: {int((sh >= obs).sum())} of {N_PERM}")
