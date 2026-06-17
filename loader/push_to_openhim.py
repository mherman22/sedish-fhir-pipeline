#!/usr/bin/env python3
"""Push the FHIR that SQLMesh produced into SEDISH via the fhir-router mediator — incrementally.

The loader no longer splits CR/SHR itself. It computes what changed (a per-resource high-water
mark over the `changed_at` column the fhir.* models carry) and POSTs FHIR transaction Bundles to
one OpenHIM channel — `/consolidated/fhir` — where the fhir-router mediator routes them:
Patient -> OpenCR (identity), clinical -> SHR, de-duping by resourceType/id.

Each cycle:
  1. identity  — page changed patients -> POST a bundle of patients      (mediator -> OpenCR)
  2. clinical  — changed enc/obs/... grouped per patient -> POST a bundle
                 (patient + its changed clinical)                        (mediator -> OpenCR + SHR)
  3. globals   — changed reference resources (Location, ...) -> POST a bundle  (mediator -> SHR)
  4. advance each resource's watermark to the max changed_at it processed (only on full success)

Idempotent: bundles PUT by id, so re-runs and overlaps converge. First run (epoch watermark)
pushes everything (the initial load). Identity is paged so the ~2.39M initial load fits in memory.
Clinical runs off its own watermarks: a patient reaches the SHR when its clinical changes (a
demographics-only change goes to OpenCR only).

Env (defaults = stock SEDISH swarm):
  FHIR_DB_HOST/PORT/USER/PASS/NAME    where SQLMesh wrote the fhir.* views (NAME=fhir)
  STATE_DB                            isolated db for the watermark table (default loader_state)
  MEDIATOR_URL                        OpenHIM channel the fhir-router mediator serves
  OPENHIM_USER/OPENHIM_PASS           OpenHIM client basic-auth (default `consolidated`, role emr)
  CLINICAL_VIEWS                      patient-scoped clinical views to bundle (empty = identity-only)
  GLOBAL_VIEWS                        non-patient-scoped reference views (Location, ...)
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
STATE_DB     = env("STATE_DB", "loader_state")
# Single endpoint: the fhir-router mediator's OpenHIM channel. It owns CR/SHR routing + dedupe.
MEDIATOR_URL = env("MEDIATOR_URL", "http://openhim-core:5001/consolidated/fhir").rstrip("/")
# The mediator's channel is an OpenHIM channel; authenticate as the `consolidated` client (role emr).
OPENHIM = (env("OPENHIM_USER", "consolidated"), env("OPENHIM_PASS", "consolidated"))
DRY_RUN = env("DRY_RUN", "") not in ("", "0", "false")
EPOCH = "1970-01-01 00:00:00"
# Page patients to avoid OOM on the ~2.39M patient initial load.
BATCH_SIZE = int(env("BATCH_SIZE", "500"))
# patient-scoped clinical views (each carries fhir_id, patient_fhir_id, changed_at).
# Add a resource = a SQLMesh model + an entry here. Empty => identity-only.
CLINICAL_VIEWS = [v.strip() for v in env("CLINICAL_VIEWS", "encounter,observation,allergy_intolerance,condition,medication_request").split(",") if v.strip()]
# global (non-patient-scoped) reference resources, re-pushed each cycle (idempotent).
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
def build_bundle(resources):
    """FHIR transaction Bundle: each resource PUT by resourceType/id. The mediator splits it."""
    return {"resourceType": "Bundle", "type": "transaction",
            "entry": [{"resource": r, "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"}}
                      for r in resources]}

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

def post_bundle(resources):
    """POST a transaction Bundle of `resources` to the mediator channel. Empty -> no-op '200'."""
    if not resources:
        return "200"
    return send(MEDIATOR_URL, "POST", OPENHIM, build_bundle(resources))

def push_identity(cur, conn):
    """Page changed patients -> POST one bundle per page to the mediator (-> OpenCR). Paged to stay
    memory-safe on the initial load. The patient watermark advances only when every page succeeded."""
    wm = watermark(cur, "patient")
    ok = fail = total = 0
    max_changed = None
    offset = 0
    while True:
        page = delta_page(cur, "patient", "fhir_id, resource", wm, BATCH_SIZE, offset)
        if not page:
            break
        patients = [json.loads(res) for _fid, res, _chg in sorted(page, key=lambda r: r[0])]
        st = post_bundle(patients)
        good = st in ("200", "201", "DRY_RUN")
        ok, fail = ok + (len(patients) if good else 0), fail + (0 if good else len(patients))
        print(f"identity: page offset={offset} n={len(patients)} -> {st}")
        batch_max = latest_changed(page)
        if batch_max and (max_changed is None or batch_max > max_changed):
            max_changed = batch_max
        total += len(page)
        offset += BATCH_SIZE
        if len(page) < BATCH_SIZE:
            break
    if not DRY_RUN:
        if fail == 0:
            if max_changed is not None:        # only commit when there was something to advance
                advance(cur, "patient", max_changed)
                conn.commit()
        else:
            print(f"  identity: holding watermark; {fail} push(es) failed; retried next cycle")
    print(f"  identity: patients={total} ok={ok} fail={fail}")
    return fail

def push_clinical(cur, conn):
    """Changed clinical grouped per patient -> POST bundle(patient + its clinical) to the mediator
    (-> OpenCR for the patient, SHR for the clinical). Driven by the clinical watermarks, so a
    patient is pushed exactly when its clinical changes. Each view's watermark advances only on
    full success."""
    wm = {v: watermark(cur, v) for v in CLINICAL_VIEWS}
    clinical = {v: delta(cur, v, "fhir_id, patient_fhir_id, resource", wm[v]) for v in CLINICAL_VIEWS}
    clin_by_pat = index_clinical(*clinical.values())
    touched = sorted(clin_by_pat)

    # fetch the current Patient resource for each touched patient (the bundle's reference target)
    patient_by_id = {}
    for i in range(0, len(touched), BATCH_SIZE):
        chunk = touched[i:i + BATCH_SIZE]
        fmt = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT fhir_id, resource FROM fhir.patient WHERE fhir_id IN ({fmt})", chunk)
        for fid, res in cur.fetchall():
            patient_by_id[fid] = json.loads(res)

    ok = fail = 0
    for pid in touched:
        patient = patient_by_id.get(pid)
        if not patient:
            # No Patient row => voided/filtered. Skip; its clinical watermark still advances
            # (we won't retry). Safe: consolidated_db creates the person before its obs/encounter
            # (FK order), so a missing patient here means intentionally excluded, not a race.
            print(f"  skip {pid}: no Patient row (voided/absent)")
            continue
        st = post_bundle([patient, *clin_by_pat[pid]])
        good = st in ("200", "201", "DRY_RUN")
        ok, fail = ok + good, fail + (not good)
        print(f"Patient/{pid}  -> {st}  changed_clinical={len(clin_by_pat[pid])}")

    if not DRY_RUN:
        if fail == 0:
            advanced = False
            for v, rows in clinical.items():
                latest = latest_changed(rows)
                if latest is not None:
                    advance(cur, v, latest)
                    advanced = True
            if advanced:                       # only commit when a watermark actually moved
                conn.commit()
        else:
            print(f"  clinical: holding watermark; {fail} push(es) failed; retried next cycle")
    deltas = " ".join(f"{v}={len(rows)}" for v, rows in clinical.items())
    print(f"  clinical: patients={len(touched)} ok={ok} fail={fail}  (Δ {deltas})")
    return fail

def push_globals(cur):
    """Global (non-patient-scoped) reference resources -> one bundle to the mediator (-> SHR).
    Re-pushed each cycle (small reference data, often without a change timestamp). Best-effort."""
    resources = []
    for view in GLOBAL_VIEWS:
        try:
            cur.execute(f"SELECT fhir_id, resource FROM fhir.{view}")
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001 — view may not exist yet
            print(f"  globals: skip {view} ({e})")
            continue
        resources.extend(json.loads(res) for _fid, res in rows)
    if not resources:
        return 0
    st = post_bundle(resources)
    good = st in ("200", "201", "DRY_RUN")
    print(f"  globals: {len(resources)} resources ({','.join(GLOBAL_VIEWS)}) -> {st}")
    return 0 if good else 1

def main():
    conn = pymysql.connect(**FHIR_DB, autocommit=False)
    with conn.cursor() as cur:
        ensure_state(cur)
        # One endpoint, the mediator routes by resource type:
        #   identity (Patient) -> OpenCR
        #   clinical           -> SHR   [skipped if CLINICAL_VIEWS is empty]
        #   globals            -> SHR
        push_identity(cur, conn)
        if CLINICAL_VIEWS:
            push_clinical(cur, conn)
        if GLOBAL_VIEWS:
            push_globals(cur)
        print(f"DONE{'  [DRY_RUN]' if DRY_RUN else ''}")

if __name__ == "__main__":
    main()
