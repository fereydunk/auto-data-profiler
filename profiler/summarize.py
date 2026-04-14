"""
AI summary generation for field profiles using the Anthropic SDK.

Called after every profile computation to produce a concise natural-language
description of the field's data characteristics and sensitivity implications.

Set ANTHROPIC_API_KEY to enable; if absent the function returns None silently.

PRIVACY INVARIANT: raw values are never included in the prompt. Only aggregate
statistics, histograms, and top values (for LOW/MEDIUM sensitivity fields) are sent.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger("profiler.summarize")

_client = None


def _get_client():
    """Lazy-init the Anthropic async client. Returns None if SDK or key is missing."""
    global _client
    if _client is None:
        try:
            import anthropic  # noqa: PLC0415
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if not api_key:
                return None
            _client = anthropic.AsyncAnthropic(api_key=api_key)
        except ImportError:
            logger.warning("anthropic package not installed — AI summaries disabled")
            return None
    return _client


def _build_prompt(profile: dict) -> str:
    """Construct the prompt for Claude from a profile dict."""
    lines = [
        f"Field: {profile['field_path']}",
        f"Topic (Kafka): {profile['topic']}",
        f"Classification tag: {profile['tag']} (sensitivity: {profile['sensitivity']})",
        f"Detected type: {profile['field_type']}",
        f"Sample size: {profile['sample_size']} records, "
        f"null rate: {profile['null_rate'] * 100:.1f}%",
    ]

    if profile["field_type"] == "numeric":
        stat_keys = [
            ("stat_min", "min"),
            ("stat_max", "max"),
            ("stat_mean", "mean"),
            ("stat_median", "median"),
            ("stat_stddev", "std dev"),
            ("stat_p25", "p25"),
            ("stat_p75", "p75"),
            ("stat_p95", "p95"),
        ]
        for key, label in stat_keys:
            v = profile.get(key)
            if v is not None:
                lines.append(f"  {label}: {v}")
        if profile.get("stat_zero_rate") is not None:
            lines.append(f"  zero rate: {profile['stat_zero_rate'] * 100:.1f}%")

    hist = profile.get("histogram", [])
    if hist:
        lines.append("Distribution (histogram buckets):")
        for bucket in hist[:10]:
            lines.append(f"  {bucket['label']}: {bucket['count']} ({bucket['pct']}%)")

    # Only include top values for non-HIGH sensitivity fields
    top_vals = profile.get("top_values", [])
    if top_vals and profile.get("sensitivity") in ("LOW", "MEDIUM"):
        lines.append("Top values:")
        for tv in top_vals[:5]:
            lines.append(f"  {tv['value']!r}: {tv['count']} ({tv['pct']}%)")

    lines.append(
        "\nWrite a concise 1-2 sentence summary of this field for a data steward. "
        "Include: what the field likely represents, its data quality (null rate, distribution), "
        "and any sensitivity or compliance note relevant to the tag. "
        "Do not invent values or assume context beyond the statistics above. "
        "Use plain language with no markdown formatting."
    )
    return "\n".join(lines)


async def generate_summary(profile: dict, *, client=None) -> Optional[str]:
    """
    Generate a natural-language AI summary for a field profile.

    Args:
        profile: dict from FieldProfile.to_dict()
        client:  optional pre-built AsyncAnthropic client (for testing)

    Returns:
        Summary string, or None if the API key is not set or the call fails.
    """
    if client is None:
        client = _get_client()
    if client is None:
        return None

    prompt = _build_prompt(profile)
    try:
        response = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1024,
            thinking={"type": "adaptive"},
            messages=[{"role": "user", "content": prompt}],
        )
        for block in response.content:
            if block.type == "text":
                return block.text.strip()
        return None
    except Exception as e:
        logger.warning(
            "AI summary generation failed for field '%s': %s",
            profile.get("field_path", "<unknown>"),
            e,
        )
        return None
