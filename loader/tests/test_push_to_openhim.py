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
    assert L.build_bundle([patient, enc, obs]) == {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {"resource": patient, "request": {"method": "PUT", "url": "Patient/p1"}},
            {"resource": enc,     "request": {"method": "PUT", "url": "Encounter/e1"}},
            {"resource": obs,     "request": {"method": "PUT", "url": "Observation/o1"}},
        ],
    }


def test_build_bundle_order_preserved():
    resources = [{"resourceType": "Patient", "id": "p1"}] + \
        [{"resourceType": "Observation", "id": f"o{i}"} for i in range(5)]
    b = L.build_bundle(resources)
    assert [e["request"]["url"] for e in b["entry"]] == \
        ["Patient/p1", "Observation/o0", "Observation/o1", "Observation/o2", "Observation/o3", "Observation/o4"]


def test_build_bundle_single_entry():
    b = L.build_bundle([{"resourceType": "Patient", "id": "p1"}])
    assert len(b["entry"]) == 1 and b["entry"][0]["request"]["url"] == "Patient/p1"


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
        elif "where patient_fhir_id in" in s and "from fhir." in s:   # fetch_clinical by patient key
            view = s.split("from fhir.", 1)[1].split()[0]
            rows = self.data["delta"].get(view, [])
            self._result = [(pid, res) for (_fid, pid, res, _chg) in rows if pid in params]
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


def _run_main(monkeypatch, data, dry_run=False, send_result="200",
              clinical_views=None, global_views=None):
    sent = []
    cur = FakeCursor(data)
    conn = FakeConn(cur)
    monkeypatch.setattr(L, "DRY_RUN", dry_run)
    if clinical_views is not None:
        monkeypatch.setattr(L, "CLINICAL_VIEWS", clinical_views)
    if global_views is not None:
        monkeypatch.setattr(L, "GLOBAL_VIEWS", global_views)
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


# patient delta rows are 3-tuples: (fhir_id, resource_json, changed_at)
def _pat_row(fhir_id, changed): return (fhir_id, _pat(fhir_id), changed)


def _bundle_ids(body):
    """resourceType/id of every entry in a posted transaction bundle."""
    return [e["request"]["url"] for e in body["entry"]]


# Everything the loader sends is a POST of a transaction Bundle to the mediator channel; the
# mediator (not the loader) splits Patient->OpenCR and clinical->SHR.

# ======================================================================
# Identity: changed patients -> bundle POSTed to the mediator
# ======================================================================
def test_identity_posts_patient_bundle_to_mediator(monkeypatch):
    data = {"watermark": {}, "patients": {},
            "delta": {"patient": [_pat_row("pA", DT1)]}}
    sent, conn, cur = _run_main(monkeypatch, data, clinical_views=[], global_views=[])
    # one POST to the mediator, authed as the OpenHIM client; bundle carries just the patient
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert sent[0][2] == L.OPENHIM
    assert _bundle_ids(sent[0][3]) == ["Patient/pA"]
    assert _advances(cur) == {"patient": DT1}
    assert conn.committed is True


def test_identity_pages_into_one_bundle_ordered_by_fhir_id(monkeypatch):
    data = {"watermark": {}, "patients": {},
            "delta": {"patient": [_pat_row("pB", DT1), _pat_row("pA", DT2)]}}
    sent, _, cur = _run_main(monkeypatch, data, clinical_views=[], global_views=[])
    assert _bundle_ids(sent[0][3]) == ["Patient/pA", "Patient/pB"]   # sorted by fhir_id
    assert _advances(cur) == {"patient": DT2}        # max changed_at across the page


def test_identity_holds_watermark_on_failure(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", DT1)]}}
    sent, conn, cur = _run_main(monkeypatch, data, send_result="ERR 500: []",
                                clinical_views=[], global_views=[])
    assert sent                          # it attempted the POST
    assert _advances(cur) == {}          # but advanced nothing
    assert conn.committed is False       # and did not commit (delta retried next cycle)


def test_identity_dry_run_posts_but_does_not_advance(monkeypatch):
    data = {"watermark": {}, "patients": {}, "delta": {"patient": [_pat_row("pA", DT1)]}}
    sent, conn, cur = _run_main(monkeypatch, data, dry_run=True, clinical_views=[], global_views=[])
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert _advances(cur) == {} and conn.committed is False


def test_identity_only_when_clinical_views_empty(monkeypatch):
    # CLINICAL_VIEWS empty => identity-only: clinical deltas are never even queried.
    data = {"watermark": {}, "patients": {},
            "delta": {"patient": [_pat_row("pA", DT1)],
                      "encounter": [("e1", "pA", _enc("e1"), DT2)]}}
    sent, _, cur = _run_main(monkeypatch, data, clinical_views=[], global_views=[])
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    delta_sqls = [s for s, _ in cur.executed if "where changed_at" in s.lower()]
    assert len(delta_sqls) == 1                      # patient only — clinical views never queried
    assert "from fhir.patient" in delta_sqls[0].lower()


# ======================================================================
# Clinical: changed clinical grouped per patient -> bundle(patient + clinical) POSTed to mediator
# ======================================================================
def test_clinical_bundles_changed_clinical_per_patient(monkeypatch):
    # no patient delta: the patient is fetched by id purely as the bundle's reference target
    data = {"watermark": {"encounter": DT0, "observation": DT0},
            "patients": {"pA": _pat("pA")},
            "delta": {"patient": [],
                      "encounter": [("e1", "pA", _enc("e1"), DT1)],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data,
                                clinical_views=["encounter", "observation"], global_views=[])
    # one POST to the mediator; patient first, then its clinical (mediator does the CR/SHR split)
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert sent[0][2] == L.OPENHIM
    assert _bundle_ids(sent[0][3]) == ["Patient/pA", "Encounter/e1", "Observation/o1"]
    assert any("from fhir.patient where fhir_id in" in s.lower() for s, _ in cur.executed)
    assert _advances(cur) == {"encounter": DT1, "observation": DT2}
    assert conn.committed is True


def test_clinical_only_bundles_patients_whose_clinical_changed(monkeypatch):
    # obs changed for pA only; pB unchanged -> only pA is bundled
    data = {"watermark": {"observation": DT0},
            "patients": {"pA": _pat("pA"), "pB": _pat("pB")},
            "delta": {"observation": [("o9", "pA", _obs("o9"), DT2)]}}
    sent, _, cur = _run_main(monkeypatch, data, clinical_views=["observation"], global_views=[])
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert set(_bundle_ids(sent[0][3])) == {"Patient/pA", "Observation/o9"}
    assert _advances(cur) == {"observation": DT2}


def test_clinical_pushes_a_new_view_allergy(monkeypatch):
    # an AllergyIntolerance changed for an existing patient -> bundled + posted, like enc/obs
    allergy = json.dumps({"resourceType": "AllergyIntolerance", "id": "al1"})
    data = {"watermark": {"allergy_intolerance": DT0},
            "patients": {"pA": _pat("pA")},
            "delta": {"allergy_intolerance": [("al1", "pA", allergy, DT2)]}}
    sent, _, cur = _run_main(monkeypatch, data,
                             clinical_views=["allergy_intolerance"], global_views=[])
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert set(_bundle_ids(sent[0][3])) == {"Patient/pA", "AllergyIntolerance/al1"}
    assert _advances(cur) == {"allergy_intolerance": DT2}


def test_clinical_skips_patient_with_no_row(monkeypatch):
    # obs for pX, but pX has no Patient row anywhere -> no push for it
    data = {"watermark": {"observation": DT0}, "patients": {},
            "delta": {"observation": [("oX", "pX", _obs("oX"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data, clinical_views=["observation"], global_views=[])
    assert sent == []                    # nothing posted (can't bundle a patient we don't have)
    # intended: a skipped (voided/absent) patient's clinical watermark still advances,
    # so it is NOT retried forever (consolidated_db creates person before obs, FK order).
    assert _advances(cur) == {"observation": DT2}
    assert conn.committed is True


def test_clinical_holds_watermark_on_failure(monkeypatch):
    data = {"watermark": {"observation": DT0}, "patients": {"pA": _pat("pA")},
            "delta": {"observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data, send_result="ERR 500: []",
                                clinical_views=["observation"], global_views=[])
    assert sent                          # it attempted the POST
    assert _advances(cur) == {}          # but advanced nothing
    assert conn.committed is False


# ======================================================================
# A full cycle: identity + clinical + globals, all POSTed to the mediator
# ======================================================================
def test_identity_and_clinical_both_post_in_one_run(monkeypatch):
    data = {"watermark": {},
            "patients": {"pA": _pat("pA")},
            "delta": {"patient": [_pat_row("pA", DT1)],
                      "encounter": [("e1", "pA", _enc("e1"), DT1)],
                      "observation": [("o1", "pA", _obs("o1"), DT2)]}}
    sent, conn, cur = _run_main(monkeypatch, data,
                                clinical_views=["encounter", "observation"], global_views=[])
    # identity bundle first, then the clinical bundle — both to the mediator
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL), ("POST", L.MEDIATOR_URL)]
    assert _bundle_ids(sent[0][3]) == ["Patient/pA"]
    assert set(_bundle_ids(sent[1][3])) == {"Patient/pA", "Encounter/e1", "Observation/o1"}
    assert _advances(cur) == {"patient": DT1, "encounter": DT1, "observation": DT2}
    assert conn.committed is True


def test_warm_posts_nothing(monkeypatch):
    data = {"watermark": {"patient": DT2, "encounter": DT2, "observation": DT2},
            "patients": {}, "delta": {"patient": [], "encounter": [], "observation": []}}
    sent, _, cur = _run_main(monkeypatch, data,
                             clinical_views=["encounter", "observation"], global_views=[])
    assert sent == []
    assert _advances(cur) == {}          # nothing to advance
    # queried the patient delta (paged) + each clinical view with the stored watermark
    delta_sqls = [s for s, _ in cur.executed if "where changed_at" in s.lower()]
    assert len(delta_sqls) == 1 + 2


def test_globals_post_bundle_to_mediator(monkeypatch):
    # global resources (Location) are bundled and POSTed to the mediator (which routes them to SHR)
    loc = json.dumps({"resourceType": "Location", "id": "11106"})
    data = {"watermark": {}, "patients": {}, "delta": {"patient": []},
            "globals": {"location": [("11106", loc)]}}
    sent, _, _ = _run_main(monkeypatch, data, clinical_views=[], global_views=["location"])
    assert [(m, u) for m, u, _, _ in sent] == [("POST", L.MEDIATOR_URL)]
    assert _bundle_ids(sent[0][3]) == ["Location/11106"]


# ======================================================================
# push_patients(keys) — targeted, key-driven push (for CDC / reconcile / backfill)
# ======================================================================
def _run_push_patients(monkeypatch, data, keys, clinical_views, send_result="200"):
    sent = []
    cur = FakeCursor(data)
    monkeypatch.setattr(L, "DRY_RUN", False)
    monkeypatch.setattr(L, "CLINICAL_VIEWS", clinical_views)
    monkeypatch.setattr(L, "send", lambda url, method, cred, body, **kw:
                        sent.append((method, url, body)) or send_result)
    ok, fail = L.push_patients(cur, keys)
    return sent, ok, fail


def test_push_patients_pushes_full_bundle_for_each_key(monkeypatch):
    # pA's FULL current state (patient + its clinical) is pushed; pB's obs is NOT (not requested)
    data = {"patients": {"pA": _pat("pA"), "pB": _pat("pB")},
            "delta": {"encounter": [("e1", "pA", _enc("e1"), DT1)],
                      "observation": [("o1", "pA", _obs("o1"), DT1), ("o9", "pB", _obs("o9"), DT1)]}}
    sent, ok, fail = _run_push_patients(monkeypatch, data, ["pA"], ["encounter", "observation"])
    assert (ok, fail) == (1, 0)
    assert [(m, u) for m, u, _ in sent] == [("POST", L.MEDIATOR_URL)]
    ids = [e["request"]["url"] for e in sent[0][2]["entry"]]
    assert ids[0] == "Patient/pA"                              # patient first
    assert set(ids) == {"Patient/pA", "Encounter/e1", "Observation/o1"}  # not pB's o9


def test_push_patients_one_bundle_per_key(monkeypatch):
    data = {"patients": {"pA": _pat("pA"), "pB": _pat("pB")}, "delta": {"observation": []}}
    sent, ok, fail = _run_push_patients(monkeypatch, data, ["pB", "pA"], ["observation"])
    assert (ok, fail) == (2, 0)
    # one POST per patient, sorted by key
    assert [_bundle_ids(b)[0] for _m, _u, b in sent] == ["Patient/pA", "Patient/pB"]


def test_push_patients_skips_missing_patient(monkeypatch):
    data = {"patients": {}, "delta": {"observation": []}}
    sent, ok, fail = _run_push_patients(monkeypatch, data, ["pX"], ["observation"])
    assert (ok, fail) == (0, 0)        # skipped, not failed
    assert sent == []


def test_push_patients_reports_failures(monkeypatch):
    data = {"patients": {"pA": _pat("pA")}, "delta": {"observation": []}}
    sent, ok, fail = _run_push_patients(monkeypatch, data, ["pA"], ["observation"], send_result="ERR 500: []")
    assert (ok, fail) == (0, 1)
