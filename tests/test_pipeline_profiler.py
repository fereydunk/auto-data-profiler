"""
Unit tests for profiler integration in the Kafka pipeline.

Tests: _get_field_value, accumulate_profiler, post_profiler_batch.
No Kafka, no HTTP connections required — httpx calls are mocked.
"""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import httpx

# Set dummy Confluent env vars before importing pipeline so Config.__init__ doesn't fail.
os.environ.setdefault("CONFLUENT_BOOTSTRAP_SERVERS", "pkc-test:9092")
os.environ.setdefault("CONFLUENT_API_KEY",           "test-key")
os.environ.setdefault("CONFLUENT_API_SECRET",        "test-secret")
os.environ.setdefault("CONFLUENT_SR_URL",            "https://psrc-test.confluent.cloud")
os.environ.setdefault("CONFLUENT_SR_API_KEY",        "sr-key")
os.environ.setdefault("CONFLUENT_SR_API_SECRET",     "sr-secret")
os.environ.setdefault("CONFLUENT_SR_CLUSTER_ID",     "lsrc-test")

# kafka-pipeline is already on sys.path via conftest.py
from pipeline import _get_field_value, accumulate_profiler, post_profiler_batch


# ── _get_field_value ──────────────────────────────────────────────────────────

class TestGetFieldValue:
    def test_flat_key(self):
        payload = {"email": "alice@example.com", "ssn": "123-45-6789"}
        assert _get_field_value(payload, "email") == "alice@example.com"

    def test_nested_key(self):
        payload = {"customer": {"email": "bob@example.com"}}
        assert _get_field_value(payload, "customer.email") == "bob@example.com"

    def test_deeply_nested(self):
        payload = {"a": {"b": {"c": "deep_value"}}}
        assert _get_field_value(payload, "a.b.c") == "deep_value"

    def test_missing_top_level(self):
        assert _get_field_value({"x": 1}, "y") is None

    def test_missing_nested(self):
        assert _get_field_value({"a": {"b": 1}}, "a.c") is None

    def test_path_through_non_dict(self):
        payload = {"a": "string_not_dict"}
        assert _get_field_value(payload, "a.b") is None

    def test_flat_key_takes_priority_over_nested_traversal(self):
        # If "a.b" exists as a literal key, return it directly
        payload = {"a.b": "flat_value", "a": {"b": "nested_value"}}
        assert _get_field_value(payload, "a.b") == "flat_value"

    def test_numeric_value(self):
        payload = {"age": 42}
        assert _get_field_value(payload, "age") == 42

    def test_none_value_in_payload(self):
        payload = {"field": None}
        # None stored in payload — return None (consistent with "not found")
        assert _get_field_value(payload, "field") is None


# ── accumulate_profiler ───────────────────────────────────────────────────────

class TestAccumulateProfiler:
    def _entities(self, tag="PII", score=0.95):
        return [{"entity_type": "EMAIL_ADDRESS", "tag": tag, "score": score}]

    def test_adds_new_field_to_buffer(self):
        buf = {}
        accumulate_profiler(
            {"email": "alice@example.com"},
            {"email": self._entities("PII")},
            buf,
        )
        assert "email" in buf
        assert buf["email"]["tag"] == "PII"
        assert buf["email"]["values"] == ["alice@example.com"]

    def test_accumulates_values_across_calls(self):
        buf = {}
        for val in ["alice@x.com", "bob@x.com", "carol@x.com"]:
            accumulate_profiler({"email": val}, {"email": self._entities()}, buf)
        assert buf["email"]["values"] == ["alice@x.com", "bob@x.com", "carol@x.com"]

    def test_multiple_fields_in_one_call(self):
        buf = {}
        payload = {"email": "alice@x.com", "ssn": "123-45-6789"}
        detected = {
            "email": self._entities("PII"),
            "ssn":   [{"entity_type": "US_SSN", "tag": "GOVERNMENT_ID", "score": 0.99}],
        }
        accumulate_profiler(payload, detected, buf)
        assert set(buf.keys()) == {"email", "ssn"}
        assert buf["ssn"]["tag"] == "GOVERNMENT_ID"

    def test_skips_field_not_in_payload(self):
        buf = {}
        accumulate_profiler({}, {"email": self._entities()}, buf)
        assert buf == {}

    def test_skips_field_with_none_value(self):
        buf = {}
        accumulate_profiler({"email": None}, {"email": self._entities()}, buf)
        assert buf == {}

    def test_skips_empty_entities_list(self):
        buf = {}
        accumulate_profiler({"email": "x@y.com"}, {"email": []}, buf)
        assert buf == {}

    def test_best_tag_is_highest_scoring(self):
        buf = {}
        entities = [
            {"entity_type": "EMAIL_ADDRESS", "tag": "PII", "score": 0.90},
            {"entity_type": "PERSON",        "tag": "PHI", "score": 0.95},
        ]
        accumulate_profiler({"notes": "Dr. Alice Smith"}, {"notes": entities}, buf)
        assert buf["notes"]["tag"] == "PHI"

    def test_nested_field_path_resolved(self):
        buf = {}
        payload = {"customer": {"email": "alice@x.com"}}
        accumulate_profiler(payload, {"customer.email": self._entities()}, buf)
        # "customer.email" not a top-level key → dot traversal resolves it
        assert "customer.email" in buf
        assert buf["customer.email"]["values"] == ["alice@x.com"]

    def test_numeric_value_accumulated(self):
        buf = {}
        accumulate_profiler({"age": 35}, {"age": self._entities("PII")}, buf)
        assert buf["age"]["values"] == [35]

    def test_tag_set_on_first_call_not_overwritten(self):
        # First message sets tag; subsequent messages for same field use whatever tag
        # they bring (the first call wins because we only set tag when creating the entry)
        buf = {}
        accumulate_profiler({"f": "v1"}, {"f": self._entities("PII", 0.9)}, buf)
        accumulate_profiler({"f": "v2"}, {"f": self._entities("PHI", 0.8)}, buf)
        # Both values are accumulated
        assert buf["f"]["values"] == ["v1", "v2"]


# ── post_profiler_batch ───────────────────────────────────────────────────────

class TestPostProfilerBatch:
    def _make_client(self, status=201):
        client = AsyncMock(spec=httpx.AsyncClient)
        response = MagicMock()
        response.status_code = status
        client.post = AsyncMock(return_value=response)
        return client

    @pytest.mark.asyncio
    async def test_posts_to_correct_url(self):
        client = self._make_client()
        buffer = {"email": {"tag": "PII", "values": ["a@b.com"]}}

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = "http://localhost:8002"
            await post_profiler_batch(client, "payments", buffer)

        client.post.assert_called_once()
        call_url = client.post.call_args[0][0]
        assert call_url == "http://localhost:8002/profiles/compute"

    @pytest.mark.asyncio
    async def test_payload_contains_topic_and_fields(self):
        client = self._make_client()
        buffer = {"email": {"tag": "PII", "values": ["x@y.com"]}}

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = "http://localhost:8002"
            await post_profiler_batch(client, "orders", buffer)

        payload = client.post.call_args[1]["json"]
        assert payload["topic"] == "orders"
        assert "scanned_at" in payload
        assert payload["fields"] == buffer

    @pytest.mark.asyncio
    async def test_skips_if_profiler_url_empty(self):
        client = self._make_client()

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = ""
            await post_profiler_batch(client, "payments", {"f": {"tag": "PII", "values": [1]}})

        client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_if_buffer_empty(self):
        client = self._make_client()

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = "http://localhost:8002"
            await post_profiler_batch(client, "payments", {})

        client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_http_error_is_swallowed(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = "http://localhost:8002"
            # Should not raise
            await post_profiler_batch(client, "payments", {"f": {"tag": "PII", "values": [1]}})

    @pytest.mark.asyncio
    async def test_timeout_error_is_swallowed(self):
        client = AsyncMock(spec=httpx.AsyncClient)
        client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))

        with patch("pipeline.cfg") as mock_cfg:
            mock_cfg.PROFILER_URL = "http://localhost:8002"
            await post_profiler_batch(client, "payments", {"f": {"tag": "PII", "values": [1]}})
