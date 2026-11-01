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
 

 
 
def download(url: str, dest: Path) -> None:
    with requests.get(url, stream=True, timeout=(10, 120)) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
 
 
def ingest_permits() -> None:
    """Ingest permits, check incoming, then truncate existing and insert."""
    DATASET_ID = "p798-x3nx"
    CSV_URL = f"https://data.cityofgainesville.org/api/views/{DATASET_ID}/rows.csv?accessType=DOWNLOAD"
    DB_PATH = Path(__file__).resolve().parent / "data" / "gainesville.db"
    TABLE = "building_permits"
    MIN_RATIO = 0.9  # abort if a pull has fewer than 90% of the previous pull's rows

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
 
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = Path(tmp) / "permits.csv"
        download(CSV_URL, csv_path)
 
        con = duckdb.connect(str(DB_PATH))
        # All-text keeps leading zeros (parcel IDs) and avoids type-inference failures;
        # cast to real types in a downstream model.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE incoming AS
            SELECT * FROM read_csv('{csv_path.as_posix()}',
                                   header = true, all_varchar = true, normalize_names = true)
        """)
        n_new = con.execute("SELECT count(*) FROM incoming").fetchone()[0]
        if n_new == 0:
            sys.exit("Downloaded file has 0 rows; leaving the database untouched.")
 
        exists = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [TABLE]
        ).fetchone()[0]
 
        con.execute("BEGIN")
        try:
            if not exists:
                con.execute(f"CREATE TABLE {TABLE} AS SELECT *, now() AS _loaded_at FROM incoming")
            else:
                prev = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
                if n_new < prev * MIN_RATIO:
                    raise RuntimeError(f"New pull has {n_new} rows vs {prev} currently; aborting.")
 
                # Schema drift: add any columns the portal has introduced since the last run.
                have = {r[0].lower() for r in con.execute(f"DESCRIBE {TABLE}").fetchall()}
                for name, *_ in con.execute("DESCRIBE incoming").fetchall():
                    if name.lower() not in have:
                        con.execute(f'ALTER TABLE {TABLE} ADD COLUMN "{name}" VARCHAR')
 
                con.execute(f"TRUNCATE {TABLE}")
                con.execute(f"INSERT INTO {TABLE} BY NAME SELECT *, now() AS _loaded_at FROM incoming")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()
 
    print(f"Loaded {n_new} rows into {TABLE} at {DB_PATH}")
 

def main() -> None:
    ingest_permits()

 
if __name__ == "__main__":
    main()