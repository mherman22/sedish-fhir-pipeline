#!/usr/bin/env python3
"""Sync the EXTERNAL (read-only) Consolidé consolidated_db into the LOCAL pipeline MySQL.

Why: SQLMesh runs one SQL statement per model and MySQL can't JOIN across servers, so the
source tables must live on the same server SQLMesh writes to. When we only have read-only
access to Consolidé (no writable schema there), we copy the source into a local MySQL and run
SQLMesh against that copy. Read-only `SELECT` is all this needs on the remote.

Per table: create it locally if absent (from the remote DDL); then copy — incrementally by
`date_updated` where that column exists (REPLACE by PK), full otherwise (small/static tables).
Idempotent; a per-table watermark lives in `<DST_DB>.sync_state`.

Env:
  SRC_HOST/SRC_PORT/SRC_USER/SRC_PASS   external Consolidé MySQL (read-only)
  SRC_DB   (default consolidated_db)
  FHIR_DB_HOST/PORT/USER/PASS           local pipeline MySQL (writable)
  DST_DB   (default consolidated_db)    local schema to sync into
  SYNC_BATCH (default 5000)             rows per insert batch
"""
import os
import re
import pymysql

def env(k, d=None): return os.environ.get(k, d)

SRC = dict(host=env("SRC_HOST"), port=int(env("SRC_PORT", "3306")),
           user=env("SRC_USER"), password=env("SRC_PASS"),
           database=env("SRC_DB", "consolidated_db"), connect_timeout=10, read_timeout=600)
DST = dict(host=env("FHIR_DB_HOST", "pipeline-db"), port=int(env("FHIR_DB_PORT", "3306")),
           user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
           connect_timeout=10)
DST_DB = env("DST_DB", "consolidated_db")
BATCH = int(env("SYNC_BATCH", "5000"))
EPOCH = "1970-01-01 00:00:00"

# Tables the SQLMesh models read — taken from external_models.yaml so it stays in lockstep.
def source_tables():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    txt = open(os.path.join(here, "external_models.yaml")).read()
    seen, out = set(), []
    for t in re.findall(r"`consolidated_db`\.`([a-z_]+)`", txt):
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out

def has_column(cur, db, table, col):
    cur.execute("""SELECT 1 FROM information_schema.columns
                   WHERE table_schema=%s AND table_name=%s AND column_name=%s LIMIT 1""", (db, table, col))
    return cur.fetchone() is not None

def ensure_state(dcur):
    dcur.execute(f"CREATE TABLE IF NOT EXISTS `{DST_DB}`.sync_state "
                 "(table_name VARCHAR(64) PRIMARY KEY, last_updated DATETIME NOT NULL)")

def watermark(dcur, table):
    dcur.execute(f"SELECT last_updated FROM `{DST_DB}`.sync_state WHERE table_name=%s", (table,))
    r = dcur.fetchone()
    return r[0].strftime("%Y-%m-%d %H:%M:%S") if r else EPOCH

def advance(dcur, table, ts):
    dcur.execute(f"INSERT INTO `{DST_DB}`.sync_state (table_name,last_updated) VALUES (%s,%s) "
                 "ON DUPLICATE KEY UPDATE last_updated=VALUES(last_updated)", (table, ts))

def sync_table(scur, dcur, table):
    # create locally from the remote DDL if missing (sql_mode='' tolerates legacy zero-dates)
    dcur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s", (DST_DB, table))
    if not dcur.fetchone():
        scur.execute(f"SHOW CREATE TABLE `{table}`")
        dcur.execute(f"USE `{DST_DB}`")
        dcur.execute(scur.fetchone()[1])
    has_du = has_column(scur, SRC["database"], table, "date_updated")
    since = watermark(dcur, table) if has_du else EPOCH
    # Incremental only AFTER a baseline full copy (watermark advanced past epoch). The first
    # sync is always full, so rows with NULL/blank date_updated aren't missed.
    incremental = has_du and since != EPOCH
    if incremental:
        scur.execute(f"SELECT * FROM `{table}` WHERE date_updated > %s", (since,))
    else:
        dcur.execute(f"TRUNCATE `{DST_DB}`.`{table}`")
        scur.execute(f"SELECT * FROM `{table}`")
    cols = [c[0] for c in scur.description]
    collist = ",".join("`" + c + "`" for c in cols)
    ph = ",".join(["%s"] * len(cols))
    verb = "REPLACE" if incremental else "INSERT"
    n = 0
    while True:
        rows = scur.fetchmany(BATCH)
        if not rows:
            break
        dcur.executemany(f"{verb} INTO `{DST_DB}`.`{table}` ({collist}) VALUES ({ph})", rows)
        n += len(rows)
    # advance the watermark to the latest date_updated on the source (so next run is incremental)
    if has_du:
        scur.execute(f"SELECT MAX(date_updated) FROM `{table}`")
        m = scur.fetchone()[0]
        if m is not None and m.strftime("%Y-%m-%d %H:%M:%S") > since:
            advance(dcur, table, m.strftime("%Y-%m-%d %H:%M:%S"))
    return n, ("incremental" if incremental else "full")

def main():
    s = pymysql.connect(**SRC)
    d = pymysql.connect(**DST, autocommit=False)
    with s.cursor() as scur, d.cursor() as dcur:
        dcur.execute("SET SESSION sql_mode=''")
        dcur.execute("SET FOREIGN_KEY_CHECKS=0")
        dcur.execute(f"CREATE DATABASE IF NOT EXISTS `{DST_DB}`")
        ensure_state(dcur)
        total = 0
        for t in source_tables():
            try:
                n, mode = sync_table(scur, dcur, t)
                d.commit()
                total += n
                print(f"  sync {t}: {n} rows ({mode})")
            except Exception as e:  # noqa: BLE001 — keep syncing the rest; surface the failure
                d.rollback()
                print(f"  sync {t}: FAILED ({e})")
        print(f"sync done: {total} rows across source tables")

if __name__ == "__main__":
    main()
