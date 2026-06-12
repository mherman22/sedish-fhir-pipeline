#!/usr/bin/env python3
"""Load the FHIR that SQLMesh produced into SEDISH, via OpenHIM.

Reads the materialized `fhir.patient` / `fhir.encounter` / `fhir.observation`
views (the SQLMesh output), then per patient:
  * IDENTITY  -> PUT  {OPENCR_URL}/Patient/{id}        (OpenCR / MPI)
  * CLINICAL  -> POST {SHR_URL}  (transaction Bundle: Patient + Encounters + Observations)

OpenCR de-duplicates / cross-links identities (we never merge here); the SHR
re-points clinical references onto the resulting golden record. Idempotent: every
write is a PUT keyed on the OpenMRS uuid, so re-runs converge.

Config is via env (defaults match a standard SEDISH swarm deployment):
  FHIR_DB_HOST/PORT/USER/PASS         where SQLMesh wrote the fhir.* views
  OPENCR_URL/OPENCR_USER/OPENCR_PASS  CR channel on OpenHIM
  SHR_URL/SHR_USER/SHR_PASS           SHR channel on OpenHIM
  DRY_RUN=1                           build the payloads, don't POST
"""
import os, json, base64, time, collections, urllib.request, urllib.error
import pymysql

def env(k, d): return os.environ.get(k, d)

FHIR_DB = dict(host=env("FHIR_DB_HOST", "fhir-mysql"), port=int(env("FHIR_DB_PORT", "3306")),
               user=env("FHIR_DB_USER", "root"), password=env("FHIR_DB_PASS", "root"),
               database=env("FHIR_DB_NAME", "fhir"))
OPENCR_URL = env("OPENCR_URL", "http://openhim-core:5001/CR/fhir").rstrip("/")
SHR_URL    = env("SHR_URL",    "http://openhim-core:5001/SHR/fhir").rstrip("/")
OPENCR = (env("OPENCR_USER", "openshr"),      env("OPENCR_PASS", "openshr"))
SHR    = (env("SHR_USER",    "shr-pipeline"), env("SHR_PASS",    "instant101"))
DRY_RUN = env("DRY_RUN", "") not in ("", "0", "false")

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

def read(cur, view):
    cur.execute(f"SELECT resource FROM fhir.{view}")
    return [json.loads(r[0]) for r in cur.fetchall()]

def main():
    conn = pymysql.connect(**FHIR_DB)
    with conn.cursor() as cur:
        patients = read(cur, "patient")
        clinical = read(cur, "encounter") + read(cur, "observation")
    by_subject = collections.defaultdict(list)
    for r in clinical:
        by_subject[r.get("subject", {}).get("reference")].append(r)

    ok = fail = 0
    for p in patients:
        pid = p["id"]
        cr = send(f"{OPENCR_URL}/Patient/{pid}", "PUT", OPENCR, p)
        mine = by_subject.get(f"Patient/{pid}", [])
        bundle = {"resourceType": "Bundle", "type": "transaction",
                  "entry": [{"resource": r, "request": {"method": "PUT", "url": f"{r['resourceType']}/{r['id']}"}}
                            for r in [p] + mine]}
        shr = send(SHR_URL, "POST", SHR, bundle)
        good = cr in ("200", "201", "DRY_RUN") and shr in ("200", "201", "DRY_RUN")
        ok, fail = ok + good, fail + (not good)
        print(f"Patient/{pid}  CR={cr}  SHR={shr}  clinical={len(mine)}")
    print(f"DONE  ok={ok} fail={fail}{'  [DRY_RUN]' if DRY_RUN else ''}")

if __name__ == "__main__":
    main()
