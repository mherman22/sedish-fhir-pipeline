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

Env (defaults = stock SEDISH swarm):
  FHIR_DB_HOST/PORT/USER/PASS/NAME    where SQLMesh wrote the fhir.* views (NAME=fhir)
  STATE_DB                            isolated db for the watermark table (default loader_state)
  OPENCR_URL/OPENCR_USER/OPENCR_PASS  CR channel on OpenHIM
  SHR_URL/SHR_USER/SHR_PASS           SHR channel on OpenHIM
  DRY_RUN=1                           preview; don't POST and don't advance the watermark
"""
import os, json, base64, time, collections, urllib.request, urllib.error
import pymysql

def env(k, d): return os.environ.get(k, d)

FHIR_DB = dict(host=env("FHIR_DB_HOST", "fhir-mysql"), port=int(env("FHIR_DB_PORT", "3306")),
               user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
               database=env("FHIR_DB_NAME", "fhir"))
STATE_DB   = env("STATE_DB", "loader_state")
OPENCR_URL = env("OPENCR_URL", "http://openhim-core:5001/CR/fhir").rstrip("/")
SHR_URL    = env("SHR_URL",    "http://openhim-core:5001/SHR/fhir").rstrip("/")
OPENCR = (env("OPENCR_USER", "openshr"),      env("OPENCR_PASS", "openshr"))
SHR    = (env("SHR_USER",    "shr-pipeline"), env("SHR_PASS",    "instant101"))
DRY_RUN = env("DRY_RUN", "") not in ("", "0", "false")
EPOCH = "1970-01-01 00:00:00"

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
                time.sleep(2 ** attempt); continue
            return f"ERR {e.code}: {e.read().decode()[:160]}"
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt); continue
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

# --- pure helpers (no I/O; unit-tested in loader/tests) -------------------
def build_bundle(patient, clinical):
    """FHIR transaction Bundle: patient + its clinical, each PUT by resourceType/id."""
    return {"resourceType": "Bundle", "type": "transaction",
            "entry": [{"resource": r, "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"}}
                      for r in [patient] + clinical]}

def index_patients(pat_rows):
    """rows (fhir_id, resource_json, changed_at) -> {fhir_id: resource_dict}."""
    return {fid: json.loads(res) for fid, res, _ in pat_rows}

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

def main():
    conn = pymysql.connect(**FHIR_DB, autocommit=False)
    with conn.cursor() as cur:
        ensure_state(cur)
        wm = {r: watermark(cur, r) for r in ("patient", "encounter", "observation")}

        pats = delta(cur, "patient",     "fhir_id, resource",                  wm["patient"])
        encs = delta(cur, "encounter",   "fhir_id, patient_fhir_id, resource", wm["encounter"])
        obs  = delta(cur, "observation", "fhir_id, patient_fhir_id, resource", wm["observation"])

        patient_by_id = index_patients(pats)
        clin_by_pat = index_clinical(encs, obs)
        touched = set(patient_by_id) | set(clin_by_pat)
        # patients touched only via clinical: fetch their current Patient resource
        missing = [p for p in touched if p not in patient_by_id]
        if missing:
            fmt = ",".join(["%s"] * len(missing))
            cur.execute(f"SELECT fhir_id, resource FROM fhir.patient WHERE fhir_id IN ({fmt})", missing)
            for fid, res in cur.fetchall(): patient_by_id[fid] = json.loads(res)

        ok = fail = 0
        for pid in sorted(touched):
            patient = patient_by_id.get(pid)
            if not patient:
                # No Patient row => the patient is voided/filtered. Skip; its clinical
                # watermark still advances (we won't retry). Safe because consolidated_db
                # creates the person before its obs/encounter (FK order), so a missing
                # patient here means intentionally excluded, not a not-yet-arrived race.
                print(f"  skip {pid}: no Patient row (voided/absent)"); continue
            cr = send(f"{OPENCR_URL}/Patient/{pid}", "PUT", OPENCR, patient)
            mine = clin_by_pat.get(pid, [])
            shr = send(SHR_URL, "POST", SHR, build_bundle(patient, mine))
            good = cr in ("200", "201", "DRY_RUN") and shr in ("200", "201", "DRY_RUN")
            ok, fail = ok + good, fail + (not good)
            print(f"Patient/{pid}  CR={cr}  SHR={shr}  changed_clinical={len(mine)}")

        if not DRY_RUN:
            if fail == 0:
                for rtype, rows in (("patient", pats), ("encounter", encs), ("observation", obs)):
                    latest = latest_changed(rows)
                    if latest is not None:
                        advance(cur, rtype, latest)
                conn.commit()
            else:
                # Do NOT advance the watermark while any push failed — the whole delta
                # is retried next cycle (idempotent). Surfaces the failure loudly instead
                # of silently dropping the records that didn't land.
                print(f"  holding watermark: {fail} push(es) failed; delta will be retried next cycle")
        print(f"DONE  patients_touched={len(touched)} ok={ok} fail={fail}"
              f"  (Δ p={len(pats)} e={len(encs)} o={len(obs)}){'  [DRY_RUN]' if DRY_RUN else ''}")

if __name__ == "__main__":
    main()
