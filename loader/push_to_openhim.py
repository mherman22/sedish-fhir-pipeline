#!/usr/bin/env python3
"""Load the FHIR that SQLMesh produced into SEDISH, via OpenHIM — incrementally.

Each run pushes only what changed since the last run. The `fhir.*` models carry a
`changed_at` column (latest consolidated-server write time); this loader keeps a
per-resource high-water mark in its own state table and, each cycle:

  1. reads patients / encounters / observations with changed_at > last watermark
  2. for every touched patient (changed itself, or with a changed encounter/obs):
       IDENTITY -> PUT  {OPENCR_URL}/Patient/{id}                (OpenCR / MPI)
       CLINICAL -> POST {SHR_URL}  transaction Bundle (patient + its *changed* clinical)
  3. advances each watermark to the max changed_at it processed

OpenCR de-dups identities; the SHR re-points clinical refs onto the golden record.
Idempotent (PUT by uuid) — re-runs and overlaps converge. First run (watermark epoch)
pushes everything, i.e. the initial load.

PHASES. MPI_ONLY (default on) is Phase 1: push only Patient identities to OpenCR — no
clinical, no SHR, no globals. The clinical models stay dormant in the repo and their
watermarks are left untouched, so Phase 2 (MPI_ONLY=0) backfills them from the epoch
when it's switched on. Set MPI_ONLY=0 to run the full identity + clinical pipeline.

Env (defaults = stock SEDISH swarm):
  FHIR_DB_HOST/PORT/USER/PASS/NAME    where SQLMesh wrote the fhir.* views (NAME=fhir)
  STATE_DB                            isolated db for the watermark table (default loader_state)
  MPI_ONLY=1                          Phase 1: Patient->OpenCR only (default). 0 = full pipeline.
  OPENHIM_USER/OPENHIM_PASS           OpenHIM client basic-auth — ONE client for both channels
                                      (default `consolidated`, role emr, allowed on /CR and /SHR)
  OPENCR_URL                          CR channel on OpenHIM (identity / MPI)
  SHR_URL                             SHR channel on OpenHIM (clinical; Phase 2 only)
  DRY_RUN=1                           preview; don't POST and don't advance the watermark
"""
import base64
import collections
import json
import os
import time
import urllib.error
import urllib.request
import pymysql

def env(k, d): return os.environ.get(k, d)

FHIR_DB = dict(host=env("FHIR_DB_HOST", "fhir-mysql"), port=int(env("FHIR_DB_PORT", "3306")),
               user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
               database=env("FHIR_DB_NAME", "fhir"))
STATE_DB   = env("STATE_DB", "loader_state")
OPENCR_URL = env("OPENCR_URL", "http://openhim-core:5001/CR/fhir").rstrip("/")
SHR_URL    = env("SHR_URL",    "http://openhim-core:5001/SHR/fhir").rstrip("/")
# /CR and /SHR are both OpenHIM channels, so we authenticate as a single OpenHIM client.
# The `consolidated` client (role 'emr') is allowed on both channels — one credential, not two.
OPENHIM = (env("OPENHIM_USER", "consolidated"), env("OPENHIM_PASS", "consolidated"))
DRY_RUN = env("DRY_RUN", "") not in ("", "0", "false")
# Phase 1 (default): push Patient identities to OpenCR only — no clinical, no SHR.
# Defaults ON unless explicitly disabled — unset OR empty both mean Phase 1, so a blank
# MPI_ONLY env can't silently turn clinical pushing on. Only "0"/"false"/"no" => Phase 2.
MPI_ONLY = env("MPI_ONLY", "1").strip().lower() not in ("0", "false", "no")
# Idempotency key per the CHARESS spec: OpenCR upserts the source record on the source key
# (mspp_code+patient_id). Must match the `source_key_system` the patient model stamps, and be
# listed in OpenCR's `internalid` systems. Requires OpenCR conditional-update support.
SOURCE_KEY_SYSTEM = env("SOURCE_KEY_SYSTEM", "http://sedish-haiti.org/fhir/source-key")
EPOCH = "1970-01-01 00:00:00"
# Phase 1 processes patients in pages to avoid OOM on the ~2.39M patient initial load.
BATCH_SIZE = int(env("BATCH_SIZE", "500"))
# patient-scoped clinical views to push (each carries fhir_id, patient_fhir_id, changed_at).
# Add a resource = a SQLMesh model + an entry here.
CLINICAL_VIEWS = [v.strip() for v in env("CLINICAL_VIEWS", "encounter,observation,allergy_intolerance,condition,medication_request").split(",") if v.strip()]
# global (non-patient-scoped) resources: pushed to the SHR directly (not bundled per
# patient, not enrolled in OpenCR). Small reference resources, re-pushed each cycle (idempotent).
GLOBAL_VIEWS = [v.strip() for v in env("GLOBAL_VIEWS", "location").split(",") if v.strip()]

def _auth(c): return "Basic " + base64.b64encode(f"{c[0]}:{c[1]}".encode()).decode()

def send(url, method, cred, body, retries=3):
    if DRY_RUN:
        return "DRY_RUN"
    data = json.dumps(body).encode()
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data, method=method,
                headers={"Content-Type": "application/fhir+json", "Authorization": _auth(cred)})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return str(r.status)
        except urllib.error.HTTPError as e:
            if 500 <= e.code < 600 and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return f"ERR {e.code}: {e.read().decode()[:160]}"
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return f"EXC {e}"

def ensure_state(cur):
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {STATE_DB}")
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {STATE_DB}.loader_state (
                      resource_type VARCHAR(32) PRIMARY KEY,
                      last_changed_at DATETIME NOT NULL)""")

def watermark(cur, rtype):
    cur.execute(f"SELECT last_changed_at FROM {STATE_DB}.loader_state WHERE resource_type=%s", (rtype,))
    row = cur.fetchone()
    return row[0].strftime("%Y-%m-%d %H:%M:%S") if row else EPOCH

def advance(cur, rtype, ts):
    cur.execute(f"""INSERT INTO {STATE_DB}.loader_state (resource_type, last_changed_at) VALUES (%s,%s)
                    ON DUPLICATE KEY UPDATE last_changed_at=VALUES(last_changed_at)""", (rtype, ts))

def delta(cur, view, cols, since):
    cur.execute(f"SELECT {cols}, changed_at FROM fhir.{view} WHERE changed_at > %s", (since,))
    return cur.fetchall()

def delta_page(cur, view, cols, since, limit, offset):
    """One page of changed rows ordered deterministically for stable LIMIT/OFFSET pagination."""
    cur.execute(
        f"SELECT {cols}, changed_at FROM fhir.{view} "
        f"WHERE changed_at > %s ORDER BY changed_at, fhir_id LIMIT %s OFFSET %s",
        (since, limit, offset),
    )
    return cur.fetchall()

# --- pure helpers (no I/O; unit-tested in loader/tests) -------------------
def build_bundle(patient, clinical):
    """FHIR transaction Bundle: patient + its clinical, each PUT by resourceType/id."""
    return {"resourceType": "Bundle", "type": "transaction",
            "entry": [{"resource": r, "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"}}
                      for r in [patient] + clinical]}

def index_patients(pat_rows):
    """rows (fhir_id, resource_json, changed_at) -> {fhir_id: resource_dict}."""
    return {fid: json.loads(res) for fid, res, _ in pat_rows}

def cr_upsert_url(mspp_code, patient_id):
    """FHIR conditional update on the source key — the CHARESS idempotency contract:
    PUT /Patient?identifier=<source_key_system>|<mspp_code>-<patient_id>. OpenCR upserts the
    source record by this key (0 matches -> create, 1 -> update), so re-runs and the parallel
    real-time feed converge without duplicating the source record."""
    return f"{OPENCR_URL}/Patient?identifier={SOURCE_KEY_SYSTEM}|{mspp_code}-{patient_id}"

def index_clinical(*row_groups):
    """rows (fhir_id, patient_fhir_id, resource_json, changed_at) -> {patient_fhir_id: [resource_dict]}."""
    out = collections.defaultdict(list)
    for rows in row_groups:
        for _, pid, res, _ in rows:
            out[pid].append(json.loads(res))
    return out

def latest_changed(rows):
    """max changed_at (last tuple element) across rows, or None when empty."""
    return max((r[-1] for r in rows), default=None)

def push_globals(cur):
    """Push global (non-patient-scoped) resources straight to the SHR by id. Idempotent;
    re-pushed each cycle (these tables are small reference data, often without a change
    timestamp). Returns the failure count. Globals never go to OpenCR."""
    pushed = ok = 0
    for view in GLOBAL_VIEWS:
        try:
            cur.execute(f"SELECT fhir_id, resource FROM fhir.{view}")
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001 — view may not exist yet
            print(f"  globals: skip {view} ({e})")
            continue
        for _fid, res in rows:
            r = json.loads(res)
            st = send(f"{SHR_URL}/{r['resourceType']}/{r['id']}", "PUT", OPENHIM, r)
            ok += st in ("200", "201", "DRY_RUN")
            pushed += 1
    if pushed:
        print(f"  globals: pushed {ok}/{pushed} ({','.join(GLOBAL_VIEWS)})")
    return pushed - ok

def main():
    conn = pymysql.connect(**FHIR_DB, autocommit=False)
    with conn.cursor() as cur:
        ensure_state(cur)

        # ---- Phase 1 (default): MPI-only. Push Patient identities to OpenCR; no clinical,
        #      no SHR, no globals. OpenCR alone does the matching/de-dup (decisionRules.json) —
        #      we are just the feeder. Clinical watermarks are left untouched so Phase 2
        #      backfills from the epoch when MPI_ONLY=0 is set. CR push gates the watermark.
        if MPI_ONLY:
            wm = watermark(cur, "patient")
            ok = fail = total = 0
            max_changed = None
            offset = 0
            while True:
                page = delta_page(cur, "patient", "fhir_id, mspp_code, patient_id, resource", wm, BATCH_SIZE, offset)
                if not page:
                    break
                # conditional upsert on the source key (mspp_code+patient_id), sorted by fhir_id
                # for deterministic ordering. The FHIR resource id stays the uuid (Phase-2 refs).
                for fhir_id, mspp_code, patient_id, res, _chg in sorted(page, key=lambda r: r[0]):
                    cr = send(cr_upsert_url(mspp_code, patient_id), "PUT", OPENHIM, json.loads(res))
                    good = cr in ("200", "201", "DRY_RUN")
                    ok, fail = ok + good, fail + (not good)
                    print(f"Patient/{fhir_id} (src {mspp_code}-{patient_id})  CR={cr}")
                batch_max = latest_changed(page)
                if batch_max and (max_changed is None or batch_max > max_changed):
                    max_changed = batch_max
                total += len(page)
                offset += BATCH_SIZE
                if len(page) < BATCH_SIZE:
                    break
            if not DRY_RUN:
                if fail == 0:
                    if max_changed is not None:
                        advance(cur, "patient", max_changed)
                    conn.commit()
                else:
                    print(f"  holding watermark: {fail} CR push(es) failed; delta retried next cycle")
            print(f"DONE  patients={total} ok={ok} fail={fail}  [MPI-only]"
                  f"{'  [DRY_RUN]' if DRY_RUN else ''}")
            return

        # ---- Phase 2: identity + clinical (dormant; set MPI_ONLY=0 to enable) ----
        wm = {r: watermark(cur, r) for r in ["patient", *CLINICAL_VIEWS]}

        pats = delta(cur, "patient", "fhir_id, resource", wm["patient"])
        # each patient-scoped clinical view (encounter, observation, allergy_intolerance, …):
        # adding a resource = a model + an entry in CLINICAL_VIEWS, nothing else here.
        clinical = {v: delta(cur, v, "fhir_id, patient_fhir_id, resource", wm[v]) for v in CLINICAL_VIEWS}

        patient_by_id = index_patients(pats)
        clin_by_pat = index_clinical(*clinical.values())
        touched = set(patient_by_id) | set(clin_by_pat)
        # patients touched only via clinical: fetch their current Patient resource
        missing = [p for p in touched if p not in patient_by_id]
        if missing:
            fmt = ",".join(["%s"] * len(missing))
            cur.execute(f"SELECT fhir_id, resource FROM fhir.patient WHERE fhir_id IN ({fmt})", missing)
            for fid, res in cur.fetchall():
                patient_by_id[fid] = json.loads(res)

        ok = fail = 0
        for pid in sorted(touched):
            patient = patient_by_id.get(pid)
            if not patient:
                # No Patient row => the patient is voided/filtered. Skip; its clinical
                # watermark still advances (we won't retry). Safe because consolidated_db
                # creates the person before its obs/encounter (FK order), so a missing
                # patient here means intentionally excluded, not a not-yet-arrived race.
                print(f"  skip {pid}: no Patient row (voided/absent)")
                continue
            cr = send(f"{OPENCR_URL}/Patient/{pid}", "PUT", OPENHIM, patient)
            mine = clin_by_pat.get(pid, [])
            shr = send(SHR_URL, "POST", OPENHIM, build_bundle(patient, mine))
            good = cr in ("200", "201", "DRY_RUN") and shr in ("200", "201", "DRY_RUN")
            ok, fail = ok + good, fail + (not good)
            print(f"Patient/{pid}  CR={cr}  SHR={shr}  changed_clinical={len(mine)}")

        push_globals(cur)   # global reference resources (Location, …) -> SHR, best-effort

        if not DRY_RUN:
            if fail == 0:
                for rtype, rows in [("patient", pats), *clinical.items()]:
                    latest = latest_changed(rows)
                    if latest is not None:
                        advance(cur, rtype, latest)
                conn.commit()
            else:
                # Do NOT advance the watermark while any push failed — the whole delta
                # is retried next cycle (idempotent). Surfaces the failure loudly instead
                # of silently dropping the records that didn't land.
                print(f"  holding watermark: {fail} push(es) failed; delta will be retried next cycle")
        deltas = "p=%d " % len(pats) + " ".join(f"{v}={len(rows)}" for v, rows in clinical.items())
        print(f"DONE  patients_touched={len(touched)} ok={ok} fail={fail}"
              f"  (Δ {deltas}){'  [DRY_RUN]' if DRY_RUN else ''}")

if __name__ == "__main__":
    main()
