"""Aggressive unit tests for the loader — no DB, no network (pymysql + urllib faked).

These assert *exact* structures, call sequences, URLs, headers, request bodies,
watermark-advance values, commit behaviour, ordering and the skip path — so any
behavioural drift breaks a test.
"""
import base64
import datetime as dt
import io
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import push_to_openhim as L  # noqa: E402

DT0 = dt.datetime(2026, 1, 1)
DT1 = dt.datetime(2026, 2, 1)
DT2 = dt.datetime(2026, 3, 1, 12, 30, 0)


# ======================================================================
# pure helpers
# ======================================================================
def test_auth_header_exact_and_handles_specials():
    assert L._auth(("openshr", "openshr")) == "Basic " + base64.b64encode(b"openshr:openshr").decode()
    assert L._auth(("u:r", "p@ss")) == "Basic " + base64.b64encode(b"u:r:p@ss").decode()


def test_build_bundle_is_exactly_correct():
    patient = {"resourceType": "Patient", "id": "p1"}
    enc = {"resourceType": "Encounter", "id": "e1"}
    obs = {"resourceType": "Observation", "id": "o1"}
    assert L.build_bundle(patient, [enc, obs]) == {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {"resource": patient, "request": {"method": "PUT", "url": "Patient/p1"}},
            {"resource": enc,     "request": {"method": "PUT", "url": "Encounter/e1"}},
            {"resource": obs,     "request": {"method": "PUT", "url": "Observation/o1"}},
        ],
    }


def test_build_bundle_patient_first_and_order_preserved():
    patient = {"resourceType": "Patient", "id": "p1"}
    clin = [{"resourceType": "Observation", "id": f"o{i}"} for i in range(5)]
    b = L.build_bundle(patient, clin)
    assert b["entry"][0]["resource"] is patient
    assert [e["request"]["url"] for e in b["entry"]] == \
        ["Patient/p1", "Observation/o0", "Observation/o1", "Observation/o2", "Observation/o3", "Observation/o4"]


def test_build_bundle_patient_only_single_entry():
    b = L.build_bundle({"resourceType": "Patient", "id": "p1"}, [])
    assert len(b["entry"]) == 1 and b["entry"][0]["request"]["url"] == "Patient/p1"


def test_index_patients_exact_mapping():
    rows = [("pA", json.dumps({"resourceType": "Patient", "id": "pA"}), DT0),
            ("pB", json.dumps({"resourceType": "Patient", "id": "pB"}), DT1)]
    assert L.index_patients(rows) == {
        "pA": {"resourceType": "Patient", "id": "pA"},
        "pB": {"resourceType": "Patient", "id": "pB"},
    }


def test_index_clinical_groups_order_and_parses():
    encs = [("e1", "pA", json.dumps({"resourceType": "Encounter", "id": "e1"}), DT0)]
    obs = [("o1", "pA", json.dumps({"resourceType": "Observation", "id": "o1"}), DT0),
           ("o2", "pB", json.dumps({"resourceType": "Observation", "id": "o2"}), DT0)]
    g = L.index_clinical(encs, obs)
    assert set(g) == {"pA", "pB"}
    # encounters indexed before observations for the same patient (group-arg order)
    assert [r["id"] for r in g["pA"]] == ["e1", "o1"]
    assert g["pB"] == [{"resourceType": "Observation", "id": "o2"}]


def test_index_clinical_empty():
    assert L.index_clinical([], []) == {}


def test_latest_changed_returns_true_max_regardless_of_order():
    rows = [("a", "x", DT1), ("b", "y", DT2), ("c", "z", DT0)]
    assert L.latest_changed(rows) == DT2
    assert L.latest_changed([("a", "x", DT0)]) == DT0
    assert L.latest_changed([]) is None


# ======================================================================
# send()
# ======================================================================
def test_send_dry_run_does_not_touch_network(monkeypatch):
    monkeypatch.setattr(L, "DRY_RUN", True)
    called = {"n": 0}
    monkeypatch.setattr(L.urllib.request, "urlopen", lambda *a, **k: called.__setitem__("n", called["n"] + 1))
    assert L.send("http://x", "PUT", ("u", "p"), {"a": 1}) == "DRY_RUN"
    assert called["n"] == 0


def test_send_success_builds_exact_request(monkeypatch):
    monkeypatch.setattr(L, "DRY_RUN", False)
    captured = {}

    class Resp:
        status = 201
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout=120):
        captured["req"], captured["timeout"] = req, timeout
        return Resp()
    monkeypatch.setattr(L.urllib.request, "urlopen", fake_urlopen)

    body = {"resourceType": "Patient", "id": "p1"}
    assert L.send("http://openhim:5001/CR/fhir/Patient/p1", "PUT", ("openshr", "secret"), body) == "201"
    req = captured["req"]
    assert req.full_url == "http://openhim:5001/CR/fhir/Patient/p1"
    assert req.method == "PUT"
    assert req.data == json.dumps(body).encode()
    assert req.get_header("Content-type") == "application/fhir+json"
    assert req.get_header("Authorization") == "Basic " + base64.b64encode(b"openshr:secret").decode()
    assert captured["timeout"] == 120


def test_send_retries_5xx_exactly_n_times_with_backoff(monkeypatch):
    monkeypatch.setattr(L, "DRY_RUN", False)
    sleeps = []
    monkeypatch.setattr(L.time, "sleep", lambda s: sleeps.append(s))
    calls = {"n": 0}

    def boom(req, timeout=120):
        calls["n"] += 1
        raise L.urllib.error.HTTPError(req.full_url, 503, "busy", {}, io.BytesIO(b"overloaded"))
    monkeypatch.setattr(L.urllib.request, "urlopen", boom)

    out = L.send("http://x", "POST", ("u", "p"), {}, retries=3)
    assert out.startswith("ERR 503") and "overloaded" in out
    assert calls["n"] == 3                 # tried exactly `retries` times
    assert sleeps == [1, 2]                # backoff 2**0, 2**1 between the 3 attempts


def test_send_no_retry_on_4xx(monkeypatch):
    monkeypatch.setattr(L, "DRY_RUN", False)
    monkeypatch.setattr(L.time, "sleep", lambda *_: (_ for _ in ()).throw(AssertionError("should not sleep")))
    calls = {"n": 0}

    def bad(req, timeout=120):
        calls["n"] += 1
        raise L.urllib.error.HTTPError(req.full_url, 409, "conflict", {}, io.BytesIO(b"dup"))
    monkeypatch.setattr(L.urllib.request, "urlopen", bad)
    out = L.send("http://x", "PUT", ("u", "p"), {}, retries=3)
    assert out.startswith("ERR 409") and calls["n"] == 1


def test_send_retries_transient_exception_then_returns_exc(monkeypatch):
    monkeypatch.setattr(L, "DRY_RUN", False)
    monkeypatch.setattr(L.time, "sleep", lambda *_: None)
    calls = {"n": 0}

    def flaky(req, timeout=120):
        calls["n"] += 1
        raise ConnectionResetError("reset")
    monkeypatch.setattr(L.urllib.request, "urlopen", flaky)
    out = L.send("http://x", "POST", ("u", "p"), {}, retries=3)
    assert out.startswith("EXC") and calls["n"] == 3


# ======================================================================
# main() against a fake DB + fake transport
# ======================================================================
class FakeCursor:
    def __init__(self, data):
        self.data, self._result, self.executed = data, [], []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        s = sql.lower()
        if "from loader_state.loader_state" in s:
            row = self.data["watermark"].get(params[0])
            self._result = [(row,)] if row else []
        elif "from fhir.patient where fhir_id in" in s:
            self._result = [(fid, self.data["patients"][fid]) for fid in params
                            if fid in self.data["patients"]]
        elif "where changed_at" in s and "from fhir." in s:
            view = s.split("from fhir.", 1)[1].split()[0]   # patient / encounter / observation / …
            all_rows = self.data["delta"].get(view, [])
            if "limit" in s and params and len(params) >= 3:
                # delta_page(since, limit, offset) — slice to simulate pagination
                limit, offset = int(params[1]), int(params[2])
                self._result = all_rows[offset:offset + limit]
            else:
                self._result = all_rows
        elif "from fhir." in s and "where" not in s:        # global full read (push_globals)
            view = s.split("from fhir.", 1)[1].split()[0]
            self._result = self.data.get("globals", {}).get(view, [])
        else:
            self._result = []
    def fetchone(self): return self._result[0] if self._result else None
    def fetchall(self): return list(self._result)


class FakeConn:
    def __init__(self, cur): self._cur, self.committed = cur, False
    def cursor(self): return self._cur
    def commit(self): self.committed = True


def _run_main(monkeypatch, data, dry_run=False, send_result="200", mpi_only=False):
    sent = []
    cur = FakeCursor(data)
    conn = FakeConn(cur)
    monkeypatch.setattr(L, "DRY_RUN", dry_run)
    monkeypatch.setattr(L, "MPI_ONLY", mpi_only)
    monkeypatch.setattr(L.pymysql, "connect", lambda **kw: conn)
    monkeypatch.setattr(L, "send", lambda url, method, cred, body, **kw:
                        sent.append((method, url, cred, body)) or
                        (send_result(method) if callable(send_result) else send_result))
    L.main()
    return sent, conn, cur


def _advances(cur):
    """{resource_type: timestamp} from the INSERT ... loader_state statements main issued."""
    return {p[0]: p[1] for sql, p in cur.executed if "insert into loader_state" in sql.lower()}


def _pat(uuid): return json.dumps({"resourceType": "Patient", "id": uuid})
def _enc(uuid): return json.dumps({"resourceType": "Encounter", "id": uuid})
def _obs(uuid): return json.dumps({"resourceType": "Observation", "id": uuid})


def test_main_cold_pushes_everything_exactly(monkeypatch):
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [("pA", _pat("pA"), DT1)],
                      "encounter": [("e1", "pA", _enc("e1"), DT1)],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data)

    # exactly two outbound calls: identity PUT then clinical POST
    assert [(m, u) for m, u, _, _ in sent] == [
        ("PUT",  f"{L.OPENCR_URL}/Patient/pA"),
        ("POST", L.SHR_URL),
    ]
    # CR PUT carries the patient resource, authenticated as the one OpenHIM client
    assert sent[0][2] == L.OPENHIM and sent[0][3] == {"resourceType": "Patient", "id": "pA"}
    # SHR POST is the full transaction bundle, same OpenHIM client (both channels), patient first
    assert sent[1][2] == L.OPENHIM
    assert sent[1][3] == {
        "resourceType": "Bundle", "type": "transaction",
        "entry": [
            {"resource": {"resourceType": "Patient", "id": "pA"},     "request": {"method": "PUT", "url": "Patient/pA"}},
            {"resource": {"resourceType": "Encounter", "id": "e1"},   "request": {"method": "PUT", "url": "Encounter/e1"}},
            {"resource": {"resourceType": "Observation", "id": "o1"}, "request": {"method": "PUT", "url": "Observation/o1"}},
        ],
    }
    # watermark advanced to the per-type max; committed
    assert _advances(cur) == {"patient": DT1, "encounter": DT1, "observation": DT2}
    assert conn.committed is True


def test_main_warm_pushes_nothing_and_advances_nothing(monkeypatch):
    data = {"watermark": {"patient": DT2, "encounter": DT2, "observation": DT2},
            "patients": {}, "delta": {"patient": [], "encounter": [], "observation": []}}
    sent, conn, cur = _run_main(monkeypatch, data)
    assert sent == []
    assert _advances(cur) == {}          # nothing to advance
    assert conn.committed is True        # commit still issued (no-op)
    # it did query the delta for patient + each clinical view with the stored watermark
    delta_sqls = [s for s, _ in cur.executed if "where changed_at" in s.lower()]
    assert len(delta_sqls) == 1 + len(L.CLINICAL_VIEWS)


def test_main_one_changed_obs_touches_only_its_patient(monkeypatch):
    # only an observation changed; patient & encounter watermarks already current.
    data = {"watermark": {"patient": DT2, "encounter": DT2, "observation": DT0},
            "patients": {"pA": _pat("pA"), "pB": _pat("pB")},
            "delta": {"patient": [], "encounter": [],
                      "observation": [("o9", "pA", _obs("o9"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data)

    assert [(m, u) for m, u, _, _ in sent] == [
        ("PUT",  f"{L.OPENCR_URL}/Patient/pA"),
        ("POST", L.SHR_URL),
    ]
    assert sent[1][3]["entry"] == [
        {"resource": {"resourceType": "Patient", "id": "pA"},     "request": {"method": "PUT", "url": "Patient/pA"}},
        {"resource": {"resourceType": "Observation", "id": "o9"}, "request": {"method": "PUT", "url": "Observation/o9"}},
    ]
    # the absent-from-delta patient was fetched by id
    assert any("from fhir.patient where fhir_id in" in s.lower() for s, _ in cur.executed)
    # only the observation watermark moved
    assert _advances(cur) == {"observation": DT2}
    assert conn.committed is True


def test_main_pushes_a_new_clinical_view_allergy(monkeypatch):
    # an AllergyIntolerance changed for an existing patient -> bundled + pushed, like enc/obs
    allergy = json.dumps({"resourceType": "AllergyIntolerance", "id": "al1"})
    data = {"watermark": {"patient": DT2, "encounter": DT2, "observation": DT2, "allergy_intolerance": DT0},
            "patients": {"pA": _pat("pA")},
            "delta": {"patient": [], "encounter": [], "observation": [],
                      "allergy_intolerance": [("al1", "pA", allergy, DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data)
    assert [(m, u) for m, u, _, _ in sent] == [("PUT", f"{L.OPENCR_URL}/Patient/pA"), ("POST", L.SHR_URL)]
    assert {e["resource"]["id"] for e in sent[1][3]["entry"]} == {"pA", "al1"}   # patient + allergy bundled
    assert _advances(cur) == {"allergy_intolerance": DT2}


def test_main_pushes_global_resources_to_shr_by_id(monkeypatch):
    # a global resource (Location) is PUT to the SHR by resourceType/id, not patient-bundled
    loc = json.dumps({"resourceType": "Location", "id": "11106"})
    data = {"watermark": {}, "patients": {},
            "delta": {"patient": [], "encounter": [], "observation": []},
            "globals": {"location": [("11106", loc)]}}
    monkeypatch.setattr(L, "GLOBAL_VIEWS", ["location"])
    sent, conn, cur = _run_main(monkeypatch, data)
    assert ("PUT", f"{L.SHR_URL}/Location/11106") in [(m, u) for m, u, _, _ in sent]
    # globals go to SHR only, never OpenCR
    assert all("/CR/" not in u for m, u, _, _ in sent)


def test_main_orders_patients_deterministically(monkeypatch):
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [("pB", _pat("pB"), DT1), ("pA", _pat("pA"), DT1)],
                      "encounter": [], "observation": []}}
    sent, _, _ = _run_main(monkeypatch, data)
    # sorted(touched) -> pA before pB regardless of delta order
    assert [u for m, u, _, _ in sent if m == "PUT"] == [
        f"{L.OPENCR_URL}/Patient/pA", f"{L.OPENCR_URL}/Patient/pB"]


def test_main_skips_clinical_with_no_patient_row(monkeypatch):
    # obs for pX, but pX has no Patient row anywhere -> no push for it
    data = {"watermark": {"observation": DT0},
            "patients": {},
            "delta": {"patient": [], "encounter": [],
                      "observation": [("oX", "pX", _obs("oX"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data)
    assert sent == []                    # nothing pushed (can't PUT a patient we don't have)
    assert conn.committed is True
    # intended: a skipped (voided/absent) patient's clinical watermark still advances,
    # so it is NOT retried forever (consolidated_db creates person before obs, FK order).
    assert _advances(cur) == {"observation": DT2}


def test_main_holds_watermark_when_a_push_fails(monkeypatch):
    # CR PUT fails for the (only) patient -> watermark must NOT advance, no commit,
    # so the delta is retried next cycle (no silent drop).
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [("pA", _pat("pA"), DT1)], "encounter": [],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    def send_result(method):
        return "ERR 500: []" if method == "PUT" else "200"
    sent, conn, cur = _run_main(monkeypatch, data, send_result=send_result)
    assert sent                      # it did attempt the push
    assert _advances(cur) == {}      # but advanced nothing
    assert conn.committed is False   # and did not commit


def test_main_advances_only_when_all_succeed(monkeypatch):
    data = {"watermark": {}, "patients": {},
            "delta": {"patient": [("pA", _pat("pA"), DT1)], "encounter": [], "observation": []}}
    _, conn, cur = _run_main(monkeypatch, data)      # all "200"
    assert _advances(cur) == {"patient": DT1} and conn.committed is True


def test_main_dry_run_pushes_but_does_not_advance_or_commit(monkeypatch):
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [("pA", _pat("pA"), DT1)], "encounter": [],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data, dry_run=True)
    assert [(m, u) for m, u, _, _ in sent] == [
        ("PUT", f"{L.OPENCR_URL}/Patient/pA"), ("POST", L.SHR_URL)]   # still computes the pushes
    assert _advances(cur) == {}          # but never advances the watermark
    assert conn.committed is False       # and never commits


# ======================================================================
# Phase 1: MPI-only mode (MPI_ONLY=1) — Patient -> OpenCR only, no SHR/clinical
# ======================================================================
# MPI delta rows are 5-tuples: (fhir_id, mspp_code, patient_id, resource_json, changed_at)
def _pat_row(fhir_id, mspp, pid, changed): return (fhir_id, mspp, pid, _pat(fhir_id), changed)


def test_mpi_only_pushes_patient_to_cr_only(monkeypatch):
    # clinical deltas are present but MUST be ignored: only the CR upsert goes out.
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [_pat_row("pA", "11106", 11, DT1)],
                      "encounter": [("e1", "pA", _enc("e1"), DT2)],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data, mpi_only=True)
    # exactly one outbound call: conditional update on the source key. No SHR POST, no globals.
    assert [(m, u) for m, u, _, _ in sent] == [("PUT", L.cr_upsert_url("11106", 11))]
    assert sent[0][2] == L.OPENHIM
    assert sent[0][3] == {"resourceType": "Patient", "id": "pA"}
    # only the patient watermark advances; clinical watermarks are left untouched for Phase 2.
    assert _advances(cur) == {"patient": DT1}
    assert conn.committed is True


def test_mpi_only_upsert_url_is_conditional_on_source_key(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", "11106", 11, DT1)]}}
    sent, _, _ = _run_main(monkeypatch, data, mpi_only=True)
    # the source key (mspp_code-patient_id), not the uuid, keys the upsert
    assert sent[0][1] == f"{L.OPENCR_URL}/Patient?identifier={L.SOURCE_KEY_SYSTEM}|11106-11"


def test_mpi_only_does_not_query_clinical_views(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", "11106", 11, DT1)]}}
    _, _, cur = _run_main(monkeypatch, data, mpi_only=True)
    delta_sqls = [s for s, _ in cur.executed if "where changed_at" in s.lower()]
    assert len(delta_sqls) == 1                      # patient only — never the clinical views
    assert "from fhir.patient" in delta_sqls[0].lower()


def test_mpi_only_orders_patients_and_pushes_each(monkeypatch):
    data = {"watermark": {},
            "patients": {},
            "delta": {"patient": [_pat_row("pB", "11106", 22, DT1), _pat_row("pA", "11106", 11, DT2)]}}
    sent, _, cur = _run_main(monkeypatch, data, mpi_only=True)
    assert [u for m, u, _, _ in sent] == [
        L.cr_upsert_url("11106", 11), L.cr_upsert_url("11106", 22)]   # sorted by fhir_id
    assert _advances(cur) == {"patient": DT2}        # max changed_at across the batch


def test_mpi_only_holds_watermark_on_cr_failure(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", "11106", 11, DT1)]}}
    sent, conn, cur = _run_main(monkeypatch, data, mpi_only=True, send_result="ERR 500: []")
    assert sent                          # it attempted the CR upsert
    assert _advances(cur) == {}          # but advanced nothing
    assert conn.committed is False       # and did not commit (delta retried next cycle)


def test_mpi_only_dry_run_pushes_but_does_not_advance(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", "11106", 11, DT1)]}}
    sent, conn, cur = _run_main(monkeypatch, data, mpi_only=True, dry_run=True)
    assert [(m, u) for m, u, _, _ in sent] == [("PUT", L.cr_upsert_url("11106", 11))]
    assert _advances(cur) == {} and conn.committed is False


def test_mpi_only_blank_or_unset_defaults_to_phase1(monkeypatch):
    # a blank/absent MPI_ONLY must NOT silently enable clinical pushing; only 0/false/no disable.
    import importlib
    cases = {None: True, "": True, "1": True, "yes": True, "0": False, "false": False, "FALSE": False, "no": False}
    for val, expected in cases.items():
        if val is None:
            monkeypatch.delenv("MPI_ONLY", raising=False)
        else:
            monkeypatch.setenv("MPI_ONLY", val)
        importlib.reload(L)
        assert L.MPI_ONLY is expected, f"MPI_ONLY={val!r} -> {L.MPI_ONLY}"
    monkeypatch.delenv("MPI_ONLY", raising=False)
    importlib.reload(L)   # restore default state for the rest of the suite
