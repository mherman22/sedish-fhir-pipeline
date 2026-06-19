#!/usr/bin/env python3
"""Sync the EXTERNAL (read-only) Consolidé consolidated_db into the LOCAL pipeline MySQL.

Why: SQLMesh runs one SQL statement per model and MySQL can't JOIN across servers, so the
source tables must live on the same server SQLMesh writes to. With only read-only access to
Consolidé we copy the source into a local MySQL and run SQLMesh against that copy. Read-only
`SELECT` is all this needs on the remote (no replication, no write).

Change detection — PER ENTRY: without the binlog there's no change stream, and a single
"since last run" high-watermark is unsafe here. The consolidated server preserves the *iSantePlus*
audit fields, so `date_changed` is the EMR edit time, NOT when the row landed in the consolidated
server — a record edited long ago but loaded in a recent batch falls outside a since-last-run
window. So we track change PER ROW: each cycle we read (primary key,
GREATEST(date_updated, date_changed, date_created)) for every source row and compare it to the
LOCAL copy, which holds the timestamp we last processed for that PK. A row is (re)copied when its
PK is new (insert) or its source timestamp is newer than the local one (update — including OpenMRS
voids, which are timestamped updates); a PK present locally but gone from the source is deleted.
The local copy IS the per-entry processed-state, so no separate watermark/state table is needed and
updates are caught whenever they arrive, regardless of a time window.

Tables with NO change timestamp (concept/concept_name/dimensions) are static reference data: copied
once, then cached (a row's edits there are rare; re-copy by clearing the local table). A table with
no primary key falls back to a full re-copy each cycle.

Env:
  SRC_HOST/SRC_PORT/SRC_USER/SRC_PASS   external Consolidé MySQL (read-only)
  SRC_DB   (default consolidated_db)
  FHIR_DB_HOST/PORT/USER/PASS           local pipeline MySQL (writable)
  DST_DB   (default consolidated_db)    local schema to sync into
  SYNC_BATCH (default 5000)             rows per copy/delete batch
  SYNC_PROGRESS_EVERY (default 50000)   emit a progress line every N rows while copying a table
"""
import os
import re
import time
import pymysql

def env(k, d=None): return os.environ.get(k, d)

def log(msg):
    # timestamped + flushed (stdout is block-buffered under `docker logs`)
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)

SRC = dict(host=env("SRC_HOST"), port=int(env("SRC_PORT", "3306")),
           user=env("SRC_USER"), password=env("SRC_PASS"),
           database=env("SRC_DB", "consolidated_db"), connect_timeout=10, read_timeout=600)
DST = dict(host=env("FHIR_DB_HOST", "pipeline-db"), port=int(env("FHIR_DB_PORT", "3306")),
           user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
           connect_timeout=10)
DST_DB = env("DST_DB", "consolidated_db")
BATCH = int(env("SYNC_BATCH", "5000"))
# Emit a progress line roughly every this many rows while copying a (large) table.
PROGRESS_EVERY = int(env("SYNC_PROGRESS_EVERY", "50000"))
# Timestamp columns, most-recent-wins: date_changed/date_updated catch edits, date_created inserts.
CHANGE_COLS = ("date_updated", "date_changed", "date_created")
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

def change_expr(scur, table):
    """SQL expr = most recent of whichever change timestamps `table` has, or None if it has none."""
    cols = [c for c in CHANGE_COLS if has_column(scur, SRC["database"], table, c)]
    if not cols:
        return None
    parts = [f"COALESCE(`{c}`, TIMESTAMP'{EPOCH}')" for c in cols]
    return f"GREATEST({', '.join(parts)})" if len(parts) > 1 else parts[0]

def primary_key_cols(dcur, table):
    """PK column name(s) of the local table — keys the per-entry diff and names rows in failure logs."""
    dcur.execute("""SELECT column_name FROM information_schema.key_column_usage
                    WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY'
                    ORDER BY ordinal_position""", (DST_DB, table))
    return [r[0] for r in dcur.fetchall()]

def ensure_table(scur, dcur, table):
    """Create the local table from the remote DDL if it's missing."""
    dcur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s", (DST_DB, table))
    if not dcur.fetchone():
        scur.execute(f"SHOW CREATE TABLE `{table}`")
        dcur.execute(f"USE `{DST_DB}`")
        dcur.execute(scur.fetchone()[1])
        dcur.connection.commit()

def pk_chg(cur, table, pk, expr, local=False):
    """{pk_tuple: change_ts} for every row (ts is None when the table has no change columns)."""
    sel = ",".join(f"`{c}`" for c in pk)
    src = f"`{DST_DB}`.`{table}`" if local else f"`{table}`"
    cur.execute(f"SELECT {sel}, {expr if expr else 'NULL'} FROM {src}")
    n = len(pk)
    return {tuple(r[:n]): r[n] for r in cur.fetchall()}

def pk_clause(pk, tuples):
    """A `col IN (...)` (single PK) or row-value `(c1,c2) IN ((..),..)` (composite) clause + params."""
    if len(pk) == 1:
        return f"`{pk[0]}` IN ({','.join(['%s'] * len(tuples))})", [t[0] for t in tuples]
    cols = "(" + ",".join(f"`{c}`" for c in pk) + ")"
    one = "(" + ",".join(["%s"] * len(pk)) + ")"
    return f"{cols} IN ({','.join([one] * len(tuples))})", [v for t in tuples for v in t]

def _write_batch(dcur, table, sql, cols, pk_idx, rows):
    """executemany; on any error retry row-by-row to name + skip the offending row(s). -> (ok, failed)."""
    conn = dcur.connection
    try:
        dcur.executemany(sql, rows)
        conn.commit()
        return len(rows), 0
    except Exception as batch_err:  # noqa: BLE001 — isolate the bad row(s); keep the good ones
        conn.rollback()
        log(f"    {table}: batch of {len(rows)} failed ({str(batch_err)[:120]}); isolating rows…")
        ok = failed = 0
        for row in rows:
            try:
                dcur.execute(sql, row)
                conn.commit()
                ok += 1
            except Exception as row_err:  # noqa: BLE001
                conn.rollback()
                failed += 1
                rk = ", ".join(f"{cols[j]}={row[j]!r}" for j in pk_idx) if pk_idx else "?"
                log(f"    {table}: SKIP row [{rk}] — {str(row_err)[:160]}")
        return ok, failed

def copy_changed(scur, dcur, table, pk, changed):
    """REPLACE the changed PKs from source into the local copy (batched, isolated). -> (copied, failed)."""
    copied = failed = logged = 0
    sql = cols = pk_idx = None
    for i in range(0, len(changed), BATCH):
        batch = changed[i:i + BATCH]
        clause, params = pk_clause(pk, batch)
        scur.execute(f"SELECT * FROM `{table}` WHERE {clause}", params)
        rows = scur.fetchall()
        if sql is None:
            cols = [c[0] for c in scur.description]
            collist = ",".join("`" + c + "`" for c in cols)
            ph = ",".join(["%s"] * len(cols))
            sql = f"REPLACE INTO `{DST_DB}`.`{table}` ({collist}) VALUES ({ph})"
            pk_idx = [cols.index(c) for c in pk if c in cols]
        ok, bad = _write_batch(dcur, table, sql, cols, pk_idx, rows)
        copied += ok
        failed += bad
        if copied - logged >= PROGRESS_EVERY:
            logged = copied
            log(f"    {table}: {copied} rows copied…")
    return copied, failed

def delete_gone(dcur, table, pk, deleted):
    """DELETE PKs that are present locally but gone from the source. -> count."""
    conn = dcur.connection
    ndel = 0
    for i in range(0, len(deleted), BATCH):
        batch = deleted[i:i + BATCH]
        clause, params = pk_clause(pk, batch)
        try:
            dcur.execute(f"DELETE FROM `{DST_DB}`.`{table}` WHERE {clause}", params)
            conn.commit()
            ndel += len(batch)
        except Exception as e:  # noqa: BLE001
            conn.rollback()
            log(f"    {table}: delete batch of {len(batch)} failed — {str(e)[:120]}")
    return ndel

def full_copy(scur, dcur, table):
    """Truncate + stream-copy the whole table (baseline, no-PK fallback, static initial). -> (copied, failed)."""
    conn = dcur.connection
    dcur.execute(f"TRUNCATE `{DST_DB}`.`{table}`")
    conn.commit()
    scur.execute(f"SELECT * FROM `{table}`")
    cols = [c[0] for c in scur.description]
    collist = ",".join("`" + c + "`" for c in cols)
    ph = ",".join(["%s"] * len(cols))
    sql = f"INSERT INTO `{DST_DB}`.`{table}` ({collist}) VALUES ({ph})"
    pk_idx = list(range(min(1, len(cols))))  # name the first column on failure
    copied = failed = logged = 0
    while True:
        rows = scur.fetchmany(BATCH)
        if not rows:
            break
        ok, bad = _write_batch(dcur, table, sql, cols, pk_idx, rows)
        copied += ok
        failed += bad
        if copied - logged >= PROGRESS_EVERY:
            logged = copied
            log(f"    {table}: {copied} rows copied…")
    return copied, failed

def sync_table(scur, dcur, table):
    ensure_table(scur, dcur, table)
    expr = change_expr(scur, table)
    pk = primary_key_cols(dcur, table)

    # static reference (no change timestamp): copy once, then cache while populated
    if expr is None:
        dcur.execute(f"SELECT EXISTS(SELECT 1 FROM `{DST_DB}`.`{table}` LIMIT 1)")
        if dcur.fetchone()[0]:
            return 0, 0, 0, "static (cached)"
        c, f = full_copy(scur, dcur, table)
        return c, f, 0, "static (initial)"

    # no primary key to diff on: full re-copy
    if not pk:
        c, f = full_copy(scur, dcur, table)
        return c, f, 0, "full (no pk)"

    # first time (empty local): a single full copy is cheaper than per-PK fetches
    loc = pk_chg(dcur, table, pk, expr, local=True)
    if not loc:
        c, f = full_copy(scur, dcur, table)
        return c, f, 0, "baseline"

    # per-entry diff: a row is changed if it's new or its source timestamp is newer than the local one
    src = pk_chg(scur, table, pk, expr)
    changed = [k for k, c in src.items()
               if k not in loc or (c is not None and (loc[k] is None or c > loc[k]))]
    deleted = [k for k in loc if k not in src]
    copied, failed = copy_changed(scur, dcur, table, pk, changed)
    ndel = delete_gone(dcur, table, pk, deleted)
    return copied, failed, ndel, "per-entry"

def main():
    s = pymysql.connect(**SRC)
    d = pymysql.connect(**DST, autocommit=False)
    with s.cursor() as scur, d.cursor() as dcur:
        dcur.execute("SET SESSION sql_mode=''")
        dcur.execute("SET FOREIGN_KEY_CHECKS=0")
        dcur.execute(f"CREATE DATABASE IF NOT EXISTS `{DST_DB}`")
        d.commit()
        tables = source_tables()
        log(f"sync: {len(tables)} source tables -> {DST['host']}/{DST_DB} (per-entry change detection)")
        total = bad = dels = 0
        started = time.monotonic()
        for t in tables:
            t0 = time.monotonic()
            try:
                copied, failed, ndel, mode = sync_table(scur, dcur, t)
                total += copied
                bad += failed
                dels += ndel
                secs = round(time.monotonic() - t0, 1)
                extra = (f", {ndel} deleted" if ndel else "") + (f", {failed} SKIPPED" if failed else "")
                log(f"  sync {t}: {copied} upserted ({mode}) in {secs}s{extra}")
            except Exception as e:  # noqa: BLE001 — keep syncing the rest; surface the failure
                d.rollback()
                log(f"  sync {t}: FAILED — {str(e)[:200]}")
        elapsed = round(time.monotonic() - started, 1)
        log(f"sync done: {total} upserted, {dels} deleted across {len(tables)} tables in {elapsed}s"
            + (f" — {bad} row(s) skipped (see SKIP lines above)" if bad else ""))

if __name__ == "__main__":
    main()
