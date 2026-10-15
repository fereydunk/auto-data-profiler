"""
Unit tests for profiler/summarize.py — AI summary generation.

All Anthropic SDK calls are mocked; no real API calls are made.
"""
import sys
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

# Add profiler to path (clear cache to avoid leaking state from other test modules)
_PROFILER = str(Path(__file__).parent.parent / "profiler")
for _mod in ("main", "store", "compute", "summarize"):
    sys.modules.pop(_mod, None)
if _PROFILER not in sys.path:
    sys.path.insert(0, _PROFILER)

import summarize  # noqa: E402 — must come after sys.path setup


# ── helpers ───────────────────────────────────────────────────────────────────

def _base_profile(**overrides) -> dict:
    defaults = {
        "topic": "payments",
        "field_path": "amount",
        "tag": "FINANCIAL",
        "sensitivity": "MEDIUM",
        "field_type": "numeric",
        "sample_size": 100,
        "null_count": 5,
        "null_rate": 0.05,
        "scanned_at": "2026-04-14T00:00:00Z",
        "stat_min": 1.0,
        "stat_max": 500.0,
        "stat_mean": 85.3,
        "stat_median": 50.0,
        "stat_stddev": 92.1,
        "stat_p25": 20.0,
        "stat_p75": 110.0,
        "stat_p95": 300.0,
        "stat_zero_rate": 0.02,
        "histogram": [
            {"label": "$0–10", "count": 10, "pct": 10.5},
            {"label": "$10–50", "count": 30, "pct": 31.6},
            {"label": "$50–200", "count": 40, "pct": 42.1},
        ],
        "top_values": [],
        "ai_summary": None,
    }
    defaults.update(overrides)
    return defaults


def _mock_client(text="This is an AI summary."):
    """Build a mock AsyncAnthropic client that returns `text`."""
    text_block = MagicMock()
    text_block.type = "text"
    text_block.text = text

    response = MagicMock()
    response.content = [text_block]

    client = AsyncMock()
    client.messages.create = AsyncMock(return_value=response)
    return client


# ── _build_prompt ─────────────────────────────────────────────────────────────

class TestBuildPrompt:
    def test_includes_field_path(self):
        p = _base_profile(field_path="customer.email", tag="PII", sensitivity="HIGH")
        prompt = summarize._build_prompt(p)
        assert "customer.email" in prompt

    def test_includes_topic(self):
        prompt = summarize._build_prompt(_base_profile(topic="orders"))
        assert "orders" in prompt

    def test_includes_tag_and_sensitivity(self):
        prompt = summarize._build_prompt(_base_profile(tag="PII", sensitivity="HIGH"))
        assert "PII" in prompt
        assert "HIGH" in prompt

    def test_includes_null_rate(self):
        prompt = summarize._build_prompt(_base_profile(null_rate=0.12))
        assert "12.0%" in prompt

    def test_numeric_stats_included(self):
        prompt = summarize._build_prompt(_base_profile(field_type="numeric", stat_mean=85.3))
        assert "mean" in prompt
        assert "85.3" in prompt

    def test_histogram_buckets_included(self):
        profile = _base_profile(
            histogram=[{"label": "$0–10", "count": 5, "pct": 10.0}]
        )
        prompt = summarize._build_prompt(profile)
        assert "$0–10" in prompt
        assert "10.0%" in prompt

    def test_top_values_included_for_low_sensitivity(self):
        profile = _base_profile(
            sensitivity="LOW",
            tag="INTERNAL",
            top_values=[{"value": "active", "count": 80, "pct": 80.0}],
        )
        prompt = summarize._build_prompt(profile)
        assert "active" in prompt

    def test_top_values_excluded_for_high_sensitivity(self):
        profile = _base_profile(
            sensitivity="HIGH",
            tag="PII",
            top_values=[{"value": "alice@example.com", "count": 1, "pct": 1.0}],
        )
        prompt = summarize._build_prompt(profile)
        assert "alice@example.com" not in prompt

    def test_top_values_excluded_for_medium_sensitivity(self):
        # MEDIUM keeps top values (not a restriction)
        profile = _base_profile(
            sensitivity="MEDIUM",
            tag="FINANCIAL",
            top_values=[{"value": "USD", "count": 90, "pct": 90.0}],
        )
        prompt = summarize._build_prompt(profile)
        assert "USD" in prompt

    def test_non_numeric_stats_not_included(self):
        profile = _base_profile(
            field_type="categorical",
            stat_mean=None,
            stat_min=None,
            stat_max=None,
        )
        prompt = summarize._build_prompt(profile)
        # Numeric stats section should not appear for categorical
        assert "mean:" not in prompt

    def test_histogram_capped_at_ten_buckets(self):
        buckets = [{"label": f"b{i}", "count": i, "pct": float(i)} for i in range(15)]
        profile = _base_profile(histogram=buckets)
        prompt = summarize._build_prompt(profile)
        assert "b9" in prompt   # 10th bucket (0-indexed)
        assert "b10" not in prompt  # 11th bucket should be cut

    def test_instruction_line_always_present(self):
        prompt = summarize._build_prompt(_base_profile())
        assert "data steward" in prompt
        assert "plain language" in prompt


# ── generate_summary ──────────────────────────────────────────────────────────

class TestGenerateSummary:
    @pytest.mark.asyncio
    async def test_returns_summary_from_client(self):
        client = _mock_client("Payments amount field: numeric, medium sensitivity.")
        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result == "Payments amount field: numeric, medium sensitivity."

    @pytest.mark.asyncio
    async def test_strips_whitespace(self):
        client = _mock_client("  Summary with leading/trailing spaces.  ")
        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result == "Summary with leading/trailing spaces."

    @pytest.mark.asyncio
    async def test_passes_correct_model(self):
        client = _mock_client()
        await summarize.generate_summary(_base_profile(), client=client)
        call_kwargs = client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-opus-4-6"

    @pytest.mark.asyncio
    async def test_uses_adaptive_thinking(self):
        client = _mock_client()
        await summarize.generate_summary(_base_profile(), client=client)
        call_kwargs = client.messages.create.call_args[1]
        assert call_kwargs.get("thinking") == {"type": "adaptive"}

    @pytest.mark.asyncio
    async def test_prompt_sent_as_user_message(self):
        client = _mock_client()
        profile = _base_profile(field_path="amount")
        await summarize.generate_summary(profile, client=client)
        call_kwargs = client.messages.create.call_args[1]
        messages = call_kwargs["messages"]
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert "amount" in messages[0]["content"]

    @pytest.mark.asyncio
    async def test_returns_none_when_client_is_none(self):
        result = await summarize.generate_summary(_base_profile(), client=None)
        # No ANTHROPIC_API_KEY set in test env → _get_client() returns None
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_api_exception(self):
        client = AsyncMock()
        client.messages.create = AsyncMock(side_effect=Exception("API error"))
        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_text_block(self):
        """If the response has no text block (e.g. only thinking blocks), return None."""
        thinking_block = MagicMock()
        thinking_block.type = "thinking"

        response = MagicMock()
        response.content = [thinking_block]

        client = AsyncMock()
        client.messages.create = AsyncMock(return_value=response)

        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_first_text_block_when_multiple(self):
        """When thinking + text blocks present, return the text block."""
        thinking_block = MagicMock()
        thinking_block.type = "thinking"

        text_block = MagicMock()
        text_block.type = "text"
        text_block.text = "Actual summary text."

        response = MagicMock()
        response.content = [thinking_block, text_block]

        client = AsyncMock()
        client.messages.create = AsyncMock(return_value=response)

        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result == "Actual summary text."

    @pytest.mark.asyncio
    async def test_exception_does_not_propagate(self):
        """A failing summary call must not raise — it returns None silently."""
        client = AsyncMock()
        client.messages.create = AsyncMock(side_effect=RuntimeError("network failure"))
        # Should not raise
        result = await summarize.generate_summary(_base_profile(), client=client)
        assert result is None


# ── _get_client ───────────────────────────────────────────────────────────────

class TestGetClient:
    def test_returns_none_without_api_key(self):
        original = summarize._client
        try:
            summarize._client = None
            with patch.dict("os.environ", {}, clear=False):
                # Ensure key is absent
                import os
                os.environ.pop("ANTHROPIC_API_KEY", None)
                result = summarize._get_client()
            assert result is None
        finally:
            summarize._client = original

    def test_returns_none_when_anthropic_not_installed(self):
        original = summarize._client
        try:
            summarize._client = None
            with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
                with patch.dict("sys.modules", {"anthropic": None}):
                    result = summarize._get_client()
            assert result is None
        finally:
            summarize._client = original
