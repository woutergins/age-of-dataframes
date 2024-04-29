import logging
from pathlib import Path

import polars as pl
import polars.selectors as cs
import pyarrow as pa
import pyarrow.dataset
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%d/%m/%y %H:%M:%S",
)

MAX_DATE = 20240316


def download_file(url: str, dir: Path):
    local_filename: Path = dir / url.split("/")[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


results = requests.request("GET", "https://aoestats.io/api/db_dumps").json()["db_dumps"]
base_dir_p = Path("./data/aoe2/data_dump_player")
base_dir_m = Path("./data/aoe2/data_dump_matches")
fetch_base = "https://aoestats.io"

for result in results:
    matches_url = result["matches_url"]
    matches = matches_url.split("%3D")
    matches = "=".join([matches[0], matches[1].split("_")[1]])
    player_url = result["players_url"]

    dir = matches.split("/")[-2].replace("-", "")
    if int(dir.split("=")[-1]) > MAX_DATE:
        continue
    dir_p: Path = base_dir_p / dir
    dir_m: Path = base_dir_m / dir
    try:
        dir_p.mkdir(parents=True, exist_ok=False)
        dir_m.mkdir(parents=True, exist_ok=False)
    except Exception:
        continue

    logger.info(f"Downloading {matches_url}...")
    download_file(fetch_base + matches_url, dir_m)
    logger.info(f"Downloading {player_url}...")
    download_file(fetch_base + player_url, dir_p)
logger.info("Downloading finished")

part = pyarrow.dataset.partitioning(
    pa.schema([("date_range", pa.int32())]), flavor="hive"
)
ds_m = pyarrow.dataset.dataset(
    "./data/aoe2/data_dump_matches/", partitioning=part, format="parquet"
)
ds_p = pyarrow.dataset.dataset(
    "./data/aoe2/data_dump_player/", partitioning=part, format="parquet"
)

datetime_columns: pl.Expr = cs.datetime()
duration_columns: pl.Expr = cs.duration()

logger.info("Loading match data")
{}
df_m = (
    pl.scan_pyarrow_dataset(ds_m)
    .select(
        pl.col("map").cast(pl.Utf8),
        pl.col("game_id").cast(pl.Int32),
        pl.col("avg_elo").cast(pl.Float32),
        pl.col("num_players").cast(pl.Int8),
        pl.col("team_0_elo").cast(pl.Float32),
        pl.col("team_1_elo").cast(pl.Float32),
        pl.col("raw_match_type").cast(pl.Int8),
        datetime_columns.dt.cast_time_unit("ms").dt.convert_time_zone(
            "Europe/Brussels"
        ),
        duration_columns.dt.total_seconds().mul(1000).cast(pl.Duration("ms")),
        pl.col("date_range")
        .cast(pl.Utf8)
        .str.to_date(format="%Y%m%d")
        .dt.truncate(every="1w", offset="6d")
        .dt.to_string(format="%Y%m%d"),
    )
    .with_columns(
        cs.string().str.replace("_", " ").str.to_titlecase(),
    )
)
logger.info("Loading data")
df_m = df_m.collect()

logger.info("Saving cleaned match data")
df_m.write_parquet(
    "./data/aoe2/matches/",
    use_pyarrow=True,
    pyarrow_options={
        "partition_cols": ["date_range"],
        "existing_data_behavior": "delete_matching",
    },
    statistics=True,
)
logger.info("Loading player data")

df_p = (
    pl.scan_pyarrow_dataset(ds_p)
    .drop("replay_summary_raw")
    .filter(pl.col("old_rating") >= 0, pl.col("new_rating") >= 0)
    .with_columns(
        pl.col("profile_id").cast(pl.Int32),
        pl.col("game_id").cast(pl.Int32),
        pl.col("team").cast(pl.Int8),
        pl.col("old_rating").cast(pl.UInt16),
        pl.col("new_rating").cast(pl.UInt16),
        pl.col("date_range")
        .cast(pl.Utf8)
        .str.to_date(format="%Y%m%d")
        .dt.truncate(every="1w", offset="6d")
        .dt.to_string(format="%Y%m%d"),
    )
)

logger.info("Collecting player data")
df_p = df_p.collect()

logger.info("Saving cleaned player data")
df_p.write_parquet(
    "./data/aoe2/player/",
    use_pyarrow=True,
    pyarrow_options={
        "partition_cols": ["date_range"],
        "existing_data_behavior": "delete_matching",
    },
    statistics=True,
)
logger.info("Done cleaning and saving data")
