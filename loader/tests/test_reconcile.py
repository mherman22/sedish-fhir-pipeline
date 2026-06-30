"""Unit tests for the reconcile (retract) step and the provenance tag — pure logic, no DB/network."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import push_to_openhim as L  # noqa: E402
import reconcile as R  # noqa: E402


# --- provenance tag (push_to_openhim.tag_source / build_bundle) -----------
def test_tag_source_adds_uniform_provenance_tag():
    r = {"resourceType": "Observation", "id": "o1"}
    L.tag_source(r)
    assert {"system": L.SOURCE_TAG_SYSTEM, "code": L.SOURCE_TAG_CODE} in r["meta"]["tag"]


def test_tag_source_is_idempotent_and_keeps_existing_tags():
    site = {"system": "http://sedish-haiti.org/fhir/mspp-site", "code": "21100"}
    r = {"resourceType": "Observation", "id": "o1", "meta": {"tag": [site]}}
    L.tag_source(r)
    L.tag_source(r)
    tags = r["meta"]["tag"]
    assert site in tags  # existing site tag preserved
    assert sum(1 for t in tags if t.get("code") == L.SOURCE_TAG_CODE) == 1  # added exactly once


def test_build_bundle_stamps_source_tag_on_every_entry():
    b = L.build_bundle([{"resourceType": "Patient", "id": "p1"},
                        {"resourceType": "Observation", "id": "o1"}])
    for e in b["entry"]:
        assert {"system": L.SOURCE_TAG_SYSTEM, "code": L.SOURCE_TAG_CODE} in e["resource"]["meta"]["tag"]


# --- compute_orphans ------------------------------------------------------
def test_compute_orphans_returns_shr_only_ids():
    # in the SHR but no longer produced by the mapper -> retract
    assert R.compute_orphans(["a", "b"], ["a", "b", "c"]) == ["c"]


def test_compute_orphans_empty_when_shr_is_subset():
    assert R.compute_orphans(["a", "b", "c"], ["a", "b"]) == []
    assert R.compute_orphans(["a"], ["a"]) == []


def test_compute_orphans_sorted_and_deduped():
    assert R.compute_orphans(["a"], ["c", "b", "b", "a"]) == ["b", "c"]


def test_compute_orphans_all_when_nothing_expected():
    assert R.compute_orphans([], ["x", "y"]) == ["x", "y"]


# --- to_entered_in_error --------------------------------------------------
def test_to_entered_in_error_flags_status_without_mutating_input():
    src = {"resourceType": "Observation", "id": "o1", "status": "final"}
    out = R.to_entered_in_error(src)
    assert out["status"] == "entered-in-error"
    assert out["id"] == "o1"
    assert src["status"] == "final"  # original untouched (works on a copy)


def test_to_entered_in_error_idempotent_returns_none():
    already = {"resourceType": "Observation", "id": "o1", "status": "entered-in-error"}
    assert R.to_entered_in_error(already) is None


# --- view->type mapping ---------------------------------------------------
def test_every_default_clinical_view_has_a_type_mapping():
    # so no managed view is silently skipped during reconcile
    for view in ["encounter", "visit", "observation", "allergy_intolerance", "condition", "medication_request"]:
        assert view in R.VIEW_TO_TYPE


def test_visit_and_encounter_share_the_encounter_type():
    assert R.VIEW_TO_TYPE["encounter"] == R.VIEW_TO_TYPE["visit"] == "Encounter"


# --- expected_ids_by_type (views sharing a type reconcile together) -------
def test_expected_ids_by_type_unions_views_of_the_same_type():
    # encounter + visit both -> Encounter: their ids MUST be unioned, else each view's rows
    # look like orphans of the other and reconcile would retract everything.
    grouped = R.expected_ids_by_type(
        [("Encounter", ["e1", "e2"]), ("Encounter", ["v1"]), ("Observation", ["o1"])])
    assert grouped == [("Encounter", ["e1", "e2", "v1"]), ("Observation", ["o1"])]


def test_expected_ids_by_type_preserves_first_seen_order_and_handles_empty():
    assert R.expected_ids_by_type([]) == []
    assert R.expected_ids_by_type([("Observation", []), ("Encounter", ["e1"])]) == [
        ("Observation", []), ("Encounter", ["e1"])]
