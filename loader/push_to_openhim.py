#!/usr/bin/env python3
"""Push the FHIR that SQLMesh produced into SEDISH via the fhir-router mediator — incrementally.

The loader no longer splits CR/SHR itself. It computes what changed — PER ENTRY, off the
audit-derived `changed_at` column the fhir.* models carry — and POSTs FHIR transaction Bundles to
one OpenHIM channel, `/consolidated/fhir`, where the fhir-router mediator routes them:
Patient -> OpenCR (identity), clinical -> SHR, de-duping by resourceType/id.

Change detection (why per-entry):
  Each fhir.* row carries `changed_at` = GREATEST(date_updated, date_created, ...) from the source
  audit columns, so an EDIT advances it just like an insert. We remember, per `fhir_id`, the
  `changed_at` we last successfully pushed (the `loader_state.pushed` table) and re-push a row
  whenever its current `changed_at` differs. This is order-independent: it catches a NEW patient,
  an UPDATED patient, and even an edit whose timestamp is older than rows already pushed (a late /
  out-of-order sync) — cases a single global high-water mark silently dropped. A row is marked
  pushed only after its bundle succeeds, so a failed push is retried next cycle and nothing is
  stranded.

Each cycle:
  1. identity  — patients whose state changed -> POST a bundle of patients      (mediator -> OpenCR)
  2. clinical  — changed enc/obs/... grouped per patient -> POST a bundle
                 (patient + its changed clinical)                        (mediator -> OpenCR + SHR)
  3. globals   — changed reference resources (Location, ...) -> POST a bundle  (mediator -> SHR)

Idempotent: bundles PUT by id, so re-runs and overlaps converge. First run (empty `pushed`) pushes
everything (the initial load). Identity is paged so the ~2.39M initial load fits in memory. Clinical
is driven off its own per-entry state: a patient reaches the SHR when its clinical changes (a
demographics-only change goes to OpenCR only).

Env (defaults = stock SEDISH swarm):
  FHIR_DB_HOST/PORT/USER/PASS/NAME    where SQLMesh wrote the fhir.* views (NAME=fhir)
  STATE_DB                            isolated db for the push state (default loader_state)
  MEDIATOR_URL                        OpenHIM channel the fhir-router mediator serves
  OPENHIM_USER/OPENHIM_PASS           OpenHIM client basic-auth (default `consolidated`, role emr)
  CLINICAL_VIEWS                      patient-scoped clinical views to bundle (empty = identity-only)
  GLOBAL_VIEWS                        non-patient-scoped reference views (Location, ...)
  DRY_RUN=1                           preview; don't POST and don't record pushed state
  SHR_FHIR_URL                        read-side SHR channel (reconcile.py only)
  SOURCE_TAG_SYSTEM/SOURCE_TAG_CODE   provenance tag stamped on every pushed resource so the
                                      reconcile step retracts only what this pipeline wrote

Retraction (deletes/voids) is a separate step — see loader/reconcile.py (off unless
RECONCILE_RETRACT_EVERY is set).
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

def log(msg):
    # flush: stdout is block-buffered when piped (docker logs)
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)

FHIR_DB = dict(host=env("FHIR_DB_HOST", "fhir-mysql"), port=int(env("FHIR_DB_PORT", "3306")),
               user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
               database=env("FHIR_DB_NAME", "fhir"))
STATE_DB     = env("STATE_DB", "loader_state")
MEDIATOR_URL = env("MEDIATOR_URL", "http://openhim-core:5001/consolidated/fhir").rstrip("/")
# Read-side SHR channel — used only by the reconcile step (reconcile.py) to find what this pipeline
# has written so it can retract resources the source no longer produces.
SHR_FHIR_URL = env("SHR_FHIR_URL", "http://openhim-core:5001/SHR/fhir").rstrip("/")
OPENHIM = (env("OPENHIM_USER", "consolidated"), env("OPENHIM_PASS", "consolidated"))
# Uniform provenance tag stamped on every resource we push. The reconcile step scopes its
# retraction to ONLY resources carrying this tag, so it can never touch data another feed
# (XDS lab sender, the hourly batch) wrote into the shared SHR.
SOURCE_TAG_SYSTEM = env("SOURCE_TAG_SYSTEM", "http://sedish-haiti.org/fhir/source")
SOURCE_TAG_CODE   = env("SOURCE_TAG_CODE", "consolidated-pipeline")
DRY_RUN = env("DRY_RUN", "") not in ("", "0", "false")
EPOCH = "1970-01-01 00:00:00"
# patient page + identity bundle size; kept small so a bundle finishes within OpenHIM's timeout
BATCH_SIZE = int(env("BATCH_SIZE", "100"))
# patient-scoped clinical views (add a resource = a model + an entry here). Empty => identity-only.
CLINICAL_VIEWS = [v.strip() for v in env("CLINICAL_VIEWS", "encounter,visit,observation,allergy_intolerance,condition,medication_request").split(",") if v.strip()]
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

def http_get(url, cred, retries=3):
    """GET FHIR JSON (used by the reconcile step to read the SHR). Returns the parsed body or None."""
    req = urllib.request.Request(url, method="GET",
            headers={"Accept": "application/fhir+json", "Authorization": _auth(cred)})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if 500 <= e.code < 600 and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            log(f"  GET {url} -> ERR {e.code}")
            return None
        except Exception as e:  # noqa: BLE001
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            log(f"  GET {url} -> EXC {e}")
            return None

def ensure_state(cur):
    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {STATE_DB}")
    except Exception:  # noqa: BLE001 — STATE_DB may be a pre-created schema we lack CREATE on
        pass
    # per-entry push state: the changed_at we last successfully pushed for each resource id.
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {STATE_DB}.pushed (
                      resource_type VARCHAR(32) NOT NULL,
                      fhir_id       VARCHAR(64) NOT NULL,
                      changed_at    DATETIME    NOT NULL,
                      PRIMARY KEY (resource_type, fhir_id))""")
    # single-timestamp markers (used by reconcile.py for its own cadence gate, key '__reconcile__').
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {STATE_DB}.loader_state (
                      resource_type VARCHAR(32) PRIMARY KEY,
                      last_changed_at DATETIME NOT NULL)""")

def advance(cur, rtype, ts):
    """Set the single-timestamp marker for `rtype` (reconcile.py's cadence gate)."""
    cur.execute(f"""INSERT INTO {STATE_DB}.loader_state (resource_type, last_changed_at) VALUES (%s,%s)
                    ON DUPLICATE KEY UPDATE last_changed_at=VALUES(last_changed_at)""", (rtype, ts))

# --- per-entry change detection -------------------------------------------
def _pending_sql(view, cols):
    """SELECT for fhir.<view> rows that are new or whose changed_at moved past what we last pushed."""
    select_cols = ", ".join(f"f.{c.strip()}" for c in cols.split(","))
    return (f"SELECT {select_cols}, f.changed_at FROM fhir.{view} f "
            f"LEFT JOIN {STATE_DB}.pushed p ON p.resource_type=%s AND p.fhir_id=f.fhir_id "
            f"WHERE p.changed_at IS NULL OR f.changed_at > p.changed_at")

def pending(cur, view, cols):
    cur.execute(_pending_sql(view, cols), (view,))
    return cur.fetchall()

def pending_page(cur, view, cols, limit, offset=0):
    """One page of pending rows, ordered by fhir_id for stable pagination."""
    cur.execute(_pending_sql(view, cols) + " ORDER BY f.fhir_id LIMIT %s OFFSET %s", (view, limit, offset))
    return cur.fetchall()

def mark_pushed(cur, rtype, items):
    """Record (fhir_id, changed_at) as pushed for `rtype` (batched upsert). `items`: iterable of pairs."""
    items = list(items)
    if not items:
        return
    for i in range(0, len(items), BATCH_SIZE):
        chunk = items[i:i + BATCH_SIZE]
        values = ",".join(["(%s,%s,%s)"] * len(chunk))
        params = [x for fid, chg in chunk for x in (rtype, fid, chg)]
        cur.execute(f"INSERT INTO {STATE_DB}.pushed (resource_type, fhir_id, changed_at) VALUES {values} "
                    f"ON DUPLICATE KEY UPDATE changed_at=VALUES(changed_at)", params)

def fetch_patients(cur, keys):
    """{fhir_id: patient_resource} for the given patient fhir_ids (chunked IN-lookups)."""
    out = {}
    for i in range(0, len(keys), BATCH_SIZE):
        chunk = keys[i:i + BATCH_SIZE]
        fmt = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT fhir_id, resource FROM fhir.patient WHERE fhir_id IN ({fmt})", chunk)
        for fid, res in cur.fetchall():
            out[fid] = json.loads(res)
    return out

def fetch_clinical(cur, keys):
    """{patient_fhir_id: [clinical resource, ...]} across CLINICAL_VIEWS for the given patients."""
    out = collections.defaultdict(list)
    for view in CLINICAL_VIEWS:
        try:
            for i in range(0, len(keys), BATCH_SIZE):
                chunk = keys[i:i + BATCH_SIZE]
                fmt = ",".join(["%s"] * len(chunk))
                cur.execute(f"SELECT patient_fhir_id, resource FROM fhir.{view} WHERE patient_fhir_id IN ({fmt})", chunk)
                for pid, res in cur.fetchall():
                    out[pid].append(json.loads(res))
        except Exception as e:  # noqa: BLE001 — view may not exist yet
            log(f"  fetch_clinical: skip {view} ({e})")
    return out

# --- pure helpers (no I/O; unit-tested in loader/tests) -------------------
def tag_source(resource):
    """Stamp the uniform provenance tag (in place) so the reconcile step can find — and only ever
    retract — what THIS pipeline wrote, never another feed's data in the shared SHR. Idempotent."""
    tags = resource.setdefault("meta", {}).setdefault("tag", [])
    if not any(t.get("system") == SOURCE_TAG_SYSTEM and t.get("code") == SOURCE_TAG_CODE for t in tags):
        tags.append({"system": SOURCE_TAG_SYSTEM, "code": SOURCE_TAG_CODE})
    return resource

def build_bundle(resources):
    """FHIR transaction Bundle: each resource tagged with our provenance + PUT by resourceType/id.
    The mediator splits it (Patient -> OpenCR, clinical -> SHR)."""
    return {"resourceType": "Bundle", "type": "transaction",
            "entry": [{"resource": tag_source(r), "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"}}
                      for r in resources]}

def post_bundle(resources):
    """POST a transaction Bundle of `resources` to the mediator channel. Empty -> no-op '200'."""
    if not resources:
        return "200"
    return send(MEDIATOR_URL, "POST", OPENHIM, build_bundle(resources))

def push_identity(cur, conn):
    """Patients whose state changed since we last pushed them -> POST one bundle per page
    (-> OpenCR). Paged to stay memory-safe on the initial load. A page is marked pushed (per entry)
    and committed only on success; a failed page stops the loop and is retried next cycle. Because a
    marked row leaves the pending set, live runs keep reading offset 0; DRY_RUN can't mark, so it
    advances the offset to preview every page."""
    ok = fail = total = pages = 0
    offset = 0
    while True:
        page = pending_page(cur, "patient", "fhir_id, resource", BATCH_SIZE, offset)
        if not page:
            break
        pages += 1
        patients = [json.loads(res) for _fid, res, _chg in sorted(page, key=lambda r: r[0])]
        t0 = time.monotonic()
        st = post_bundle(patients)
        ms = int((time.monotonic() - t0) * 1000)
        good = st in ("200", "201", "DRY_RUN")
        log(f"  identity[{'OK ' if good else 'ERR'}] page={pages} n={len(patients)} {ms}ms -> {st}")
        if not good:
            fail += len(patients)
            break                                   # stop; unmarked rows are retried next cycle
        ok += len(patients)
        total += len(page)
        if DRY_RUN:
            offset += BATCH_SIZE                     # nothing gets marked; step through to preview all
        else:
            mark_pushed(cur, "patient", [(r[0], r[-1]) for r in page])
            conn.commit()                            # marked rows drop out -> keep reading offset 0
        if len(page) < BATCH_SIZE:
            break
    log(f"  identity: done patients={total} ok={ok} fail={fail} pages={pages} (BATCH_SIZE={BATCH_SIZE})")
    return fail

def _commit_marks(cur, marks):
    """Mark a patient's clinical entries pushed, grouped by view. `marks`: [(view, fhir_id, changed_at)]."""
    by_view = collections.defaultdict(list)
    for v, fid, chg in marks:
        by_view[v].append((fid, chg))
    for v, items in by_view.items():
        mark_pushed(cur, v, items)

def push_clinical(cur, conn):
    """Clinical that changed since we last pushed it, grouped per patient -> POST bundle
    (patient + its changed clinical) to the mediator (-> OpenCR for the patient, SHR for the
    clinical). Per-entry: a clinical row is marked pushed only when its patient's bundle succeeds,
    so an edit to any clinical resource re-pushes that patient regardless of global ordering."""
    pending_rows = {v: pending(cur, v, "fhir_id, patient_fhir_id, resource") for v in CLINICAL_VIEWS}
    clin_by_pat = collections.defaultdict(list)      # pid -> [resource dict]
    marks_by_pat = collections.defaultdict(list)     # pid -> [(view, fhir_id, changed_at)]
    for v, rows in pending_rows.items():
        for fid, pid, res, chg in rows:
            clin_by_pat[pid].append(json.loads(res))
            marks_by_pat[pid].append((v, fid, chg))
    touched = sorted(clin_by_pat)

    patient_by_id = fetch_patients(cur, touched)     # bundle reference target

    ok = fail = skipped = 0
    committed = False
    t0 = time.monotonic()
    for pid in touched:
        patient = patient_by_id.get(pid)
        if not patient:
            # voided/filtered patient — not pushable; mark its clinical pushed so it doesn't retry
            # forever (a later real change advances changed_at and re-triggers it).
            skipped += 1
            log(f"  clinical[SKIP] {pid}: no Patient row (voided/absent)")
            if not DRY_RUN:
                _commit_marks(cur, marks_by_pat[pid])
                committed = True
            continue
        st = post_bundle([patient, *clin_by_pat[pid]])
        good = st in ("200", "201", "DRY_RUN")
        if good:
            ok += 1
            if not DRY_RUN:
                _commit_marks(cur, marks_by_pat[pid])
                committed = True
        else:
            fail += 1
            log(f"  clinical[ERR] Patient/{pid} clin={len(clin_by_pat[pid])} -> {st}")
    if committed:
        conn.commit()
    ms = int((time.monotonic() - t0) * 1000)
    deltas = " ".join(f"{v}={len(rows)}" for v, rows in pending_rows.items())
    log(f"  clinical: done patients={len(touched)} ok={ok} fail={fail} skipped={skipped} {ms}ms (Δ {deltas})")
    return fail

def push_patients(cur, keys):
    """Targeted push: for each patient key, POST its FULL current bundle (patient + all clinical) to
    the mediator, regardless of pushed state. This is the key-driven path an event/CDC consumer
    calls (resolve changed patient -> push it), and is also handy for manual reconcile/backfill.
    Idempotent (PUT by id). No state recorded — the caller owns its scope. Returns (ok, fail)."""
    keys = sorted(set(keys))
    patients = fetch_patients(cur, keys)
    clinical = fetch_clinical(cur, keys)
    ok = fail = skipped = 0
    for pid in keys:
        patient = patients.get(pid)
        if not patient:
            skipped += 1
            log(f"  push_patients[SKIP] {pid}: no Patient row")
            continue
        st = post_bundle([patient, *clinical.get(pid, [])])
        good = st in ("200", "201", "DRY_RUN")
        ok, fail = ok + good, fail + (not good)
        if not good:
            log(f"  push_patients[ERR] Patient/{pid} -> {st}")
    log(f"  push_patients: requested={len(keys)} ok={ok} fail={fail} skipped={skipped}")
    return ok, fail

def push_globals(cur):
    """Global (non-patient-scoped) reference resources -> one bundle to the mediator (-> SHR).
    Re-pushed each cycle (small reference data, often without a change timestamp). Best-effort."""
    resources = []
    for view in GLOBAL_VIEWS:
        try:
            cur.execute(f"SELECT fhir_id, resource FROM fhir.{view}")
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001 — view may not exist yet
            log(f"  globals[SKIP] {view} ({e})")
            continue
        resources.extend(json.loads(res) for _fid, res in rows)
    if not resources:
        return 0
    st = post_bundle(resources)
    good = st in ("200", "201", "DRY_RUN")
    log(f"  globals[{'OK ' if good else 'ERR'}] {len(resources)} resources "
        f"({','.join(GLOBAL_VIEWS)}) -> {st}")
    return 0 if good else 1

def main():
    conn = pymysql.connect(**FHIR_DB, autocommit=False)
    started = time.monotonic()
    with conn.cursor() as cur:
        ensure_state(cur)
        # One endpoint, the mediator routes by resource type:
        #   identity (Patient) -> OpenCR
        #   clinical           -> SHR   [skipped if CLINICAL_VIEWS is empty]
        #   globals            -> SHR
        log(f"cycle start -> {MEDIATOR_URL}  (mode={'DRY_RUN' if DRY_RUN else 'live'})")
        fails = push_identity(cur, conn)
        if CLINICAL_VIEWS:
            fails += push_clinical(cur, conn)
        if GLOBAL_VIEWS:
            fails += push_globals(cur)
        secs = round(time.monotonic() - started, 1)
        verdict = "clean" if fails == 0 else f"{fails} failure(s) — will retry"
        log(f"cycle done in {secs}s — {verdict}{'  [DRY_RUN]' if DRY_RUN else ''}")

if __name__ == "__main__":
    main()
