#!/usr/bin/env python3
"""Retract clinical the source no longer produces — so the SHR mirrors the consolidated server.

The forward loader (push_to_openhim.py) only ever inserts/updates: it pushes rows present in the
`fhir.*` views. When a record is voided or moved to another patient at the source, it drops out of
`fhir.*`, but the copy already written to the SHR is left behind (a stale orphan). Updates that keep
the same id self-correct via PUT-by-id; this step handles the *removal* half.

Each run, per managed clinical type:
  1. expected = the resource ids the mapper currently produces        (SELECT fhir_id FROM fhir.<view>)
  2. actual   = the ids THIS pipeline has written to the SHR          (SHR search by our provenance _tag)
  3. orphans  = actual - expected
  4. retract each orphan by marking it `status = entered-in-error` (PUT via the mediator)

Safety:
  - Scoped by our provenance tag (push_to_openhim.tag_source), so it can only ever touch resources
    THIS pipeline wrote — never another feed's data (XDS lab sender, hourly batch) in the shared SHR.
  - `entered-in-error` rather than hard delete: reversible, auditable (HAPI keeps history), and the
    consolidated IPS excludes it. A re-appearing source row simply gets re-pushed (PUT-by-id).
  - Off by default. Enable with RECONCILE_RETRACT_EVERY=<seconds> (e.g. 3600). The cadence is held
    in the loader_state table so the continuous loop can call this every cycle cheaply.

Env (in addition to push_to_openhim's): RECONCILE_RETRACT_EVERY (default 0 = off).
"""
import datetime as dt
import urllib.parse

import pymysql

import push_to_openhim as L

RECONCILE_EVERY = int(L.env("RECONCILE_RETRACT_EVERY", "0"))
SHR_PAGE = int(L.env("RECONCILE_SHR_PAGE", "200"))

# managed clinical view -> FHIR resource type (only these are ever reconciled).
# Several views can share a type (encounter + visit both -> Encounter); retract() unions
# their expected ids per type before comparing against the SHR (see expected_ids_by_type).
VIEW_TO_TYPE = {
    "encounter": "Encounter",
    "visit": "Encounter",
    "observation": "Observation",
    "allergy_intolerance": "AllergyIntolerance",
    "condition": "Condition",
    "medication_request": "MedicationRequest",
    "medication_statement": "MedicationStatement",
    "procedure": "Procedure",
    "immunization": "Immunization",
    "diagnostic_report": "DiagnosticReport",
}

# --- pure helpers (no I/O; unit-tested) -----------------------------------
def compute_orphans(expected_ids, actual_ids):
    """ids the SHR holds (actual) that the mapper no longer produces (expected) -> retract."""
    return sorted(set(actual_ids) - set(expected_ids))

def to_entered_in_error(resource):
    """A copy of `resource` flagged entered-in-error; None if it already is (nothing to do)."""
    if resource.get("status") == "entered-in-error":
        return None
    out = dict(resource)
    out["status"] = "entered-in-error"
    return out

def expected_ids_by_type(view_ids):
    """Group expected ids by FHIR resource type so views that share a type reconcile together.

    `view_ids`: ordered [(rtype, [ids]), ...] (one entry per managed view). Returns ordered
    [(rtype, [ids…]), ...] unioning the ids of every view of the same type. This is essential:
    encounter and visit both produce Encounter, so reconciling per-view would make each view's
    rows look like orphans of the other and retract everything. First-seen type order is kept
    so the per-type pass (and its log line) is deterministic."""
    order, acc = [], {}
    for rtype, ids in view_ids:
        if rtype not in acc:
            acc[rtype] = []
            order.append(rtype)
        acc[rtype].extend(ids)
    return [(rtype, acc[rtype]) for rtype in order]

# --- I/O ------------------------------------------------------------------
def expected_ids_for_view(cur, view):
    cur.execute(f"SELECT fhir_id FROM fhir.{view}")
    return [row[0] for row in cur.fetchall()]

def shr_ids_for_type(rtype):
    """Every id of `rtype` THIS pipeline has written to the SHR (paged search by provenance _tag)."""
    tag = f"{L.SOURCE_TAG_SYSTEM}|{L.SOURCE_TAG_CODE}"
    url = (f"{L.SHR_FHIR_URL}/{rtype}?_tag={urllib.parse.quote(tag, safe='')}"
           f"&_elements=id&_count={SHR_PAGE}")
    ids = []
    while url:
        body = L.http_get(url, L.OPENHIM)
        if not body:
            break
        for entry in body.get("entry", []):
            rid = (entry.get("resource") or {}).get("id")
            if rid:
                ids.append(rid)
        url = next((lk.get("url") for lk in body.get("link", []) if lk.get("relation") == "next"), None)
    return ids

def retract(cur):
    """Retract orphans across every managed clinical type. Returns the count retracted."""
    total = 0
    # collect each managed view's expected ids, then union by resource type — so two views of
    # the same type (encounter + visit -> Encounter) are reconciled as one expected set.
    view_ids = []
    for view in L.CLINICAL_VIEWS:
        rtype = VIEW_TO_TYPE.get(view)
        if not rtype:
            L.log(f"  reconcile: skip {view} (no resource-type mapping)")
            continue
        try:
            view_ids.append((rtype, expected_ids_for_view(cur, view)))
        except Exception as e:  # noqa: BLE001 — view may not exist yet
            L.log(f"  reconcile: skip {view} ({e})")
            continue
    for rtype, expected in expected_ids_by_type(view_ids):
        actual = shr_ids_for_type(rtype)
        orphans = compute_orphans(expected, actual)
        retracted = 0
        for oid in orphans:
            res = L.http_get(f"{L.SHR_FHIR_URL}/{rtype}/{urllib.parse.quote(str(oid), safe='')}", L.OPENHIM)
            if not res or res.get("resourceType") != rtype:
                continue
            eie = to_entered_in_error(res)
            if eie is None:
                continue
            st = L.post_bundle([eie])
            if st in ("200", "201", "DRY_RUN"):
                retracted += 1
            else:
                L.log(f"  reconcile[ERR] {rtype}/{oid} -> {st}")
        total += retracted
        L.log(f"  reconcile[{rtype}] shr={len(actual)} fhir={len(expected)} "
              f"orphans={len(orphans)} retracted={retracted}")
    return total

def due(cur):
    cur.execute(f"SELECT last_changed_at FROM {L.STATE_DB}.loader_state WHERE resource_type='__reconcile__'")
    row = cur.fetchone()
    if not row:
        return True
    return (dt.datetime.utcnow() - row[0]).total_seconds() >= RECONCILE_EVERY

def mark(cur):
    L.advance(cur, "__reconcile__", dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

def main():
    if RECONCILE_EVERY <= 0:
        return  # disabled
    conn = pymysql.connect(**L.FHIR_DB, autocommit=False)
    with conn.cursor() as cur:
        L.ensure_state(cur)
        if not due(cur):
            return
        L.log(f"reconcile start — retract orphans (every {RECONCILE_EVERY}s, mode="
              f"{'DRY_RUN' if L.DRY_RUN else 'live'})")
        n = retract(cur)
        if not L.DRY_RUN:
            mark(cur)
            conn.commit()
        L.log(f"reconcile done — retracted {n}")

if __name__ == "__main__":
    main()
