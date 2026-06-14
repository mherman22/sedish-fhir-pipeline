"""Unit tests for the Kafka driver — no Kafka, no subprocess (both faked)."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import run_kafka as K  # noqa: E402


class RC:
    def __init__(self, code): self.returncode = code


def test_run_cycle_runs_both_steps_in_order(monkeypatch):
    calls = []
    monkeypatch.setattr(K.subprocess, "run", lambda cmd, **k: calls.append(cmd) or RC(0))
    assert K.run_cycle(3) is True
    assert calls == list(K.CYCLE_STEPS)            # sqlmesh run, then loader — in order


def test_run_cycle_stops_at_first_failure(monkeypatch):
    calls = []

    def fake(cmd, **k):
        calls.append(cmd)
        return RC(0 if "sqlmesh" in cmd else 1)    # transform ok, loader fails
    monkeypatch.setattr(K.subprocess, "run", fake)
    assert K.run_cycle(1) is False
    assert calls == list(K.CYCLE_STEPS)            # both attempted, loader returned non-zero


def test_run_cycle_aborts_before_loader_if_transform_fails(monkeypatch):
    calls = []
    monkeypatch.setattr(K.subprocess, "run", lambda cmd, **k: calls.append(cmd) or RC(1))
    assert K.run_cycle(1) is False
    assert calls == [list(K.CYCLE_STEPS)[0]]       # loader never invoked after transform failed


class FakeConsumer:
    """poll() returns each queued batch once, then {} (idle)."""
    def __init__(self, batches): self._batches = list(batches)
    def poll(self, timeout_ms=0):
        return self._batches.pop(0) if self._batches else {}


def test_drain_collapses_a_burst_into_one_count(monkeypatch):
    monkeypatch.setattr(K, "DEBOUNCE", 0.05)
    # after the first 2 events, two more polls deliver 3 then 1, then idle
    c = FakeConsumer([{"tp": [1, 1, 1]}, {"tp": [1]}])
    assert K.drain(c, first_count=2) == 6          # 2 + 3 + 1


def test_drain_returns_first_count_when_idle(monkeypatch):
    monkeypatch.setattr(K, "DEBOUNCE", 0.05)
    assert K.drain(FakeConsumer([]), first_count=4) == 4
