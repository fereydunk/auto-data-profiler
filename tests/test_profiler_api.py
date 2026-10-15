"""
Integration tests for the Auto Data Profiler FastAPI service (port 8002).
Uses a real SQLite file in tmp_path — no mocks, no HTTP calls.
"""
import sys
import pytest
from pathlib import Path
from unittest.mock import patch

# profiler/ modules share names with review-api (main, store).
# Insert profiler path and flush module cache so tests get the right files.
_PROFILER = str(Path(__file__).parent.parent / "profiler")
_PROFILER_MODULES = ("main", "store", "compute")


def _load_profiler():
    """Ensure profiler is at the front of sys.path and reload its modules."""
    if _PROFILER in sys.path:
        sys.path.remove(_PROFILER)
    sys.path.insert(0, _PROFILER)
    for name in _PROFILER_MODULES:
        sys.modules.pop(name, None)

    import store as store_module
    import main as app_module
    return app_module, store_module


from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path):
    """Each test gets a fresh SQLite file and a fresh app module."""
    app_module, _ = _load_profiler()

    db_file = str(tmp_path / "test_profiles.db")
    with patch.object(app_module, "DB_PATH", db_file):
        with TestClient(app_module.app) as c:
            yield c


# ── /health ────────────────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok", "version": "1.0.0"}


# ── /dashboard ─────────────────────────────────────────────────────────────────

class TestDashboard:
    def test_returns_html(self, client):
        resp = client.get("/dashboard")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Auto Data Profiler" in resp.text
        assert "chart.js" in resp.text.lower()


# ── /topics (empty) ────────────────────────────────────────────────────────────

class TestTopicsEmpty:
    def test_empty_initially(self, client):
        resp = client.get("/topics")
        assert resp.status_code == 200
        assert resp.json() == []


# ── /profiles (empty) ─────────────────────────────────────────────────────────

class TestProfilesEmpty:
    def test_unknown_topic_returns_empty_list(self, client):
        resp = client.get("/profiles?topic=nonexistent")
        assert resp.status_code == 200
        assert resp.json() == []


# ── POST /profiles/compute ─────────────────────────────────────────────────────

class TestComputeProfiles:
    def _compute(self, client, topic="payments", fields=None):
        fields = fields or {
            "customer.salary": {"tag": "FINANCIAL", "values": [50_000, 75_000, 100_000]},
            "customer.country": {"tag": "LOCATION", "values": ["US", "CA", "US", "UK", "US"]},
        }
        return client.post("/profiles/compute", json={
            "topic": topic,
            "scanned_at": "2026-01-01T00:00:00Z",
            "fields": fields,
        })

    def test_returns_201(self, client):
        resp = self._compute(client)
        assert resp.status_code == 201

    def test_profiled_count_matches_fields(self, client):
        resp = self._compute(client)
        body = resp.json()
        assert body["profiled"] == 2
        assert set(body["fields"]) == {"customer.salary", "customer.country"}

    def test_topic_appears_in_topics_list(self, client):
        self._compute(client, topic="orders")
        topics = client.get("/topics").json()
        assert "orders" in topics

    def test_multiple_topics(self, client):
        self._compute(client, topic="payments")
        self._compute(client, topic="orders")
        topics = client.get("/topics").json()
        assert set(topics) == {"payments", "orders"}

    def test_profiles_retrievable_after_compute(self, client):
        self._compute(client, topic="payments")
        profiles = client.get("/profiles?topic=payments").json()
        assert len(profiles) == 2
        paths = {p["field_path"] for p in profiles}
        assert paths == {"customer.salary", "customer.country"}

    def test_numeric_field_has_stats(self, client):
        self._compute(client, topic="payments")
        profiles = client.get("/profiles?topic=payments").json()
        salary = next(p for p in profiles if p["field_path"] == "customer.salary")
        assert salary["field_type"] == "numeric"
        assert salary["stat_mean"] is not None
        assert salary["stat_min"] is not None
        assert salary["stat_max"] is not None

    def test_categorical_field_has_histogram(self, client):
        self._compute(client, topic="payments")
        profiles = client.get("/profiles?topic=payments").json()
        country = next(p for p in profiles if p["field_path"] == "customer.country")
        assert country["field_type"] == "categorical"
        assert len(country["histogram"]) > 0

    def test_high_sensitivity_no_top_values(self, client):
        self._compute(client, fields={
            "customer.ssn": {"tag": "PII", "values": ["123-45-6789", "987-65-4321"] * 10},
        })
        profiles = client.get("/profiles?topic=payments").json()
        ssn = profiles[0]
        assert ssn["sensitivity"] == "HIGH"
        assert ssn["top_values"] == []

    def test_null_rate_computed_correctly(self, client):
        self._compute(client, fields={
            "field_with_nulls": {"tag": "LOW", "values": [1, None, 2, None]},
        })
        profiles = client.get("/profiles?topic=payments").json()
        p = profiles[0]
        assert p["null_count"] == 2
        assert abs(p["null_rate"] - 0.5) < 0.001

    def test_upsert_updates_existing_profile(self, client):
        # First scan
        self._compute(client, fields={"f": {"tag": "LOW", "values": [1, 2, 3]}})
        profiles_v1 = client.get("/profiles?topic=payments").json()
        assert profiles_v1[0]["sample_size"] == 3

        # Second scan with more data
        self._compute(client, fields={"f": {"tag": "LOW", "values": [1, 2, 3, 4, 5, 6]}})
        profiles_v2 = client.get("/profiles?topic=payments").json()
        assert len(profiles_v2) == 1  # still one row
        assert profiles_v2[0]["sample_size"] == 6

    def test_empty_fields_dict_returns_zero(self, client):
        resp = client.post("/profiles/compute", json={
            "topic": "empty",
            "scanned_at": "2026-01-01T00:00:00Z",
            "fields": {},
        })
        assert resp.status_code == 201
        assert resp.json()["profiled"] == 0


# ── POST /profiles (pre-computed) ─────────────────────────────────────────────

class TestStorePrecomputed:
    def _profile_payload(self, **overrides):
        base = {
            "topic": "payments",
            "field_path": "customer.age",
            "tag": "PII",
            "sensitivity": "HIGH",
            "field_type": "numeric",
            "sample_size": 100,
            "null_count": 5,
            "null_rate": 0.05,
            "scanned_at": "2026-01-01T00:00:00Z",
        }
        base.update(overrides)
        return base

    def test_returns_201(self, client):
        resp = client.post("/profiles", json=self._profile_payload())
        assert resp.status_code == 201
        assert resp.json() == {"status": "ok"}

    def test_stored_profile_is_retrievable(self, client):
        client.post("/profiles", json=self._profile_payload())
        profiles = client.get("/profiles?topic=payments").json()
        assert len(profiles) == 1
        assert profiles[0]["field_path"] == "customer.age"
        assert profiles[0]["sample_size"] == 100

    def test_upsert_replaces_existing(self, client):
        client.post("/profiles", json=self._profile_payload(sample_size=100))
        client.post("/profiles", json=self._profile_payload(sample_size=500))
        profiles = client.get("/profiles?topic=payments").json()
        assert len(profiles) == 1
        assert profiles[0]["sample_size"] == 500

    def test_with_optional_stats(self, client):
        payload = self._profile_payload(
            field_type="numeric",
            stat_min=18.0,
            stat_max=90.0,
            stat_mean=42.5,
            stat_median=40.0,
            stat_stddev=15.0,
            stat_p25=30.0,
            stat_p75=55.0,
            stat_p95=75.0,
            histogram=[{"label": "18-34", "count": 30, "pct": 30.0}],
        )
        client.post("/profiles", json=payload)
        p = client.get("/profiles?topic=payments").json()[0]
        assert p["stat_min"] == 18.0
        assert p["stat_max"] == 90.0
        assert len(p["histogram"]) == 1

    def test_with_ai_summary(self, client):
        payload = self._profile_payload(ai_summary="This field stores customer age in years.")
        client.post("/profiles", json=payload)
        p = client.get("/profiles?topic=payments").json()[0]
        assert p["ai_summary"] == "This field stores customer age in years."


# ── GET /profiles sorting ──────────────────────────────────────────────────────

class TestProfilesSorting:
    def test_all_three_sensitivities_returned(self, client):
        # Store returns profiles sorted alphabetically by sensitivity (HIGH < LOW < MEDIUM).
        # The meaningful HIGH→MEDIUM→LOW display sort is done client-side in the dashboard.
        for tag, field in [("LOCATION", "f_low"), ("FINANCIAL", "f_med"), ("PII", "f_high")]:
            client.post("/profiles/compute", json={
                "topic": "t",
                "scanned_at": "2026-01-01T00:00:00Z",
                "fields": {field: {"tag": tag, "values": ["x"] * 5}},
            })
        profiles = client.get("/profiles?topic=t").json()
        assert len(profiles) == 3
        sensitivities = {p["sensitivity"] for p in profiles}
        assert sensitivities == {"HIGH", "MEDIUM", "LOW"}

    def test_sorted_alphabetically_by_sensitivity_then_field(self, client):
        # The store sorts by (sensitivity, field_path) alphabetically.
        # HIGH < LOW < MEDIUM alphabetically.
        for tag, field in [("LOCATION", "f_low"), ("FINANCIAL", "f_med"), ("PII", "f_high")]:
            client.post("/profiles/compute", json={
                "topic": "t",
                "scanned_at": "2026-01-01T00:00:00Z",
                "fields": {field: {"tag": tag, "values": ["x"] * 5}},
            })
        profiles = client.get("/profiles?topic=t").json()
        sensitivities = [p["sensitivity"] for p in profiles]
        assert sensitivities == sorted(sensitivities)  # alphabetical: HIGH, LOW, MEDIUM
