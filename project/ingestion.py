"""Ingest the City of Gainesville Building Permits dataset (Socrata p798-x3nx) into DuckDB.
 
Each run appends a full snapshot to raw_building_permits (all columns as text, plus
_batch_id / _loaded_at / _source_url) and refreshes a view with the latest snapshot.
 
Requires: pip install duckdb requests
"""
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
 
import duckdb
import requests
 
DATASET_ID = "p798-x3nx"
CSV_URL = f"https://data.cityofgainesville.org/api/views/{DATASET_ID}/rows.csv?accessType=DOWNLOAD"
DB_PATH = Path(__file__).resolve().parent / "data" / "gainesville.db"
RAW_TABLE = "raw_building_permits"
LATEST_VIEW = "building_permits_latest"
MIN_RATIO = 0.9  # abort if a pull has fewer than 90% of the previous pull's rows
 
 
def download(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, 120)) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
 
 
def ingest_permits() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
 
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = Path(tmp) / "permits.csv"
        download(CSV_URL, csv_path)
 
        conn = duckdb.connect(str(DB_PATH))
        # All-text keeps leading zeros (parcel IDs) and avoids type-inference failures;
        # cast to real types in a downstream staging model.
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE incoming AS
            SELECT * FROM read_csv('{csv_path.as_posix()}',
                                   header = true, all_varchar = true, normalize_names = true)
        """)
        n_new = conn.execute("SELECT count(*) FROM incoming").fetchone()[0]
        if n_new == 0:
            sys.exit("Downloaded file has 0 rows; leaving the database untouched.")
 
        stamp = f"'{batch_id}' AS _batch_id, now() AS _loaded_at, '{CSV_URL}' AS _source_url"
        exists = conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [RAW_TABLE]
        ).fetchone()[0]
 
        conn.execute("BEGIN")
        try:
            if not exists:
                conn.execute(f"CREATE TABLE {RAW_TABLE} AS SELECT *, {stamp} FROM incoming")
            else:
                prev = conn.execute(
                    f"SELECT count(*) FROM {RAW_TABLE} "
                    f"WHERE _batch_id = (SELECT max(_batch_id) FROM {RAW_TABLE})"
                ).fetchone()[0]
                if n_new < prev * MIN_RATIO:
                    raise RuntimeError(f"New pull has {n_new} rows vs {prev} previously; aborting.")
 
                # Schema drift: add any columns the portal has introduced since the last run.
                have = {r[0].lower() for r in conn.execute(f"DESCRIBE {RAW_TABLE}").fetchall()}
                for name, *_ in conn.execute("DESCRIBE incoming").fetchall():
                    if name.lower() not in have:
                        conn.execute(f'ALTER TABLE {RAW_TABLE} ADD COLUMN "{name}" VARCHAR')
 
                conn.execute(f"INSERT INTO {RAW_TABLE} BY NAME SELECT *, {stamp} FROM incoming")
 
            conn.execute(f"""
                CREATE OR REPLACE VIEW {LATEST_VIEW} AS
                SELECT * EXCLUDE (_batch_id, _loaded_at, _source_url)
                FROM {RAW_TABLE}
                WHERE _batch_id = (SELECT max(_batch_id) FROM {RAW_TABLE})
            """)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
 
        total = conn.execute(f"SELECT count(*) FROM {RAW_TABLE}").fetchone()[0]
        conn.close()
 
    print(f"Batch {batch_id}: loaded {n_new} rows into {RAW_TABLE} ({total} total) at {DB_PATH}")
 

def main() -> None:
    pass

 
if __name__ == "__main__":
    main()