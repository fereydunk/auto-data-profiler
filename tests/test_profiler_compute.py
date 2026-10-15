"""
Unit tests for profiler/compute.py — statistical profiling engine.
Pure Python, no HTTP or DB required.
"""
import sys
import math
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "profiler"))
import compute


# ── sensitivity() ──────────────────────────────────────────────────────────────

class TestSensitivity:
    @pytest.mark.parametrize("tag", ["PII", "PHI", "PCI", "CREDENTIALS", "GOVERNMENT_ID", "BIOMETRIC", "GENETIC"])
    def test_high_tags(self, tag):
        assert compute.sensitivity(tag) == "HIGH"

    @pytest.mark.parametrize("tag", ["FINANCIAL", "NPI"])
    def test_medium_tags(self, tag):
        assert compute.sensitivity(tag) == "MEDIUM"

    @pytest.mark.parametrize("tag", ["LOCATION", "MINOR"])
    def test_low_tags(self, tag):
        assert compute.sensitivity(tag) == "LOW"


# ── _to_float() ────────────────────────────────────────────────────────────────

class TestToFloat:
    def test_integer(self):
        assert compute._to_float(42) == 42.0

    def test_float(self):
        assert compute._to_float(3.14) == pytest.approx(3.14)

    def test_numeric_string(self):
        assert compute._to_float("99.5") == 99.5

    def test_currency_string(self):
        assert compute._to_float("$1,234.56") == pytest.approx(1234.56)

    def test_percent_string(self):
        assert compute._to_float("75%") == 75.0

    def test_none_returns_none(self):
        assert compute._to_float(None) is None

    def test_bool_returns_none(self):
        assert compute._to_float(True) is None

    def test_non_numeric_string_returns_none(self):
        assert compute._to_float("alice@example.com") is None

    def test_empty_string_returns_none(self):
        assert compute._to_float("") is None


# ── _detect_type() ─────────────────────────────────────────────────────────────

class TestDetectType:
    def test_numeric(self):
        values = [1, 2, 3, 4.5, 6, 7]
        assert compute._detect_type("amount", "FINANCIAL", values) == "numeric"

    def test_boolean_true_false_strings(self):
        values = ["true", "false", "true", "true"]
        assert compute._detect_type("is_active", "LOW", values) == "boolean"

    def test_boolean_yes_no(self):
        values = ["yes", "no", "yes"]
        assert compute._detect_type("opted_in", "LOW", values) == "boolean"

    def test_boolean_zero_one(self):
        values = ["0", "1", "1", "0"]
        assert compute._detect_type("flag", "LOW", values) == "boolean"

    def test_categorical_low_unique_ratio(self):
        values = ["US"] * 40 + ["CA"] * 20 + ["UK"] * 10  # <30% unique
        assert compute._detect_type("country", "LOW", values) == "categorical"

    def test_categorical_small_sample(self):
        # ≤20 total values → categorical regardless of unique ratio
        values = ["a", "b", "c", "d"]
        assert compute._detect_type("status", "LOW", values) == "categorical"

    def test_freetext_phi_tag(self):
        values = ["Patient reported mild symptoms"]
        assert compute._detect_type("notes", "PHI", values) == "freetext"

    def test_freetext_high_word_count(self):
        # values with >= 5 words → freetext
        values = ["This is a long free text note that describes something"] * 5
        assert compute._detect_type("description", "LOW", values) == "freetext"

    def test_string_high_cardinality(self):
        values = [f"user_{i}@example.com" for i in range(50)]  # high unique ratio
        assert compute._detect_type("email", "PII", values) == "string"

    def test_empty_returns_string(self):
        assert compute._detect_type("x", "LOW", []) == "string"

    def test_mixed_numeric_high_hit_rate(self):
        # 90%+ parseable → numeric even with one bad value
        values = [str(i) for i in range(19)] + ["N/A"]
        assert compute._detect_type("score", "LOW", values) == "numeric"


# ── profile_field() — numeric ──────────────────────────────────────────────────

class TestProfileNumeric:
    def _profile(self, values, field_path="salary", tag="FINANCIAL"):
        return compute.profile_field("payments", field_path, tag, values, "2026-01-01T00:00:00Z")

    def test_basic_stats(self):
        p = self._profile([10, 20, 30, 40, 50])
        assert p.stat_min == 10.0
        assert p.stat_max == 50.0
        assert p.stat_mean == 30.0
        assert p.stat_median == 30.0
        assert p.stat_zero_rate == 0.0
        assert p.field_type == "numeric"
        assert p.sensitivity == "MEDIUM"

    def test_null_handling(self):
        p = self._profile([10, None, 30, None, 50])
        assert p.null_count == 2
        assert p.null_rate == pytest.approx(0.4, abs=1e-4)
        assert p.sample_size == 5

    def test_zero_rate(self):
        p = self._profile([0, 0, 10, 20])
        assert p.stat_zero_rate == 0.5

    def test_percentiles(self):
        vals = list(range(1, 101))  # 1..100
        p = self._profile(vals)
        assert p.stat_p25 is not None
        assert p.stat_p75 is not None
        assert p.stat_p95 is not None
        assert p.stat_p25 < p.stat_median < p.stat_p75

    def test_histogram_not_empty(self):
        p = self._profile(list(range(100)))
        assert len(p.histogram) > 0
        for bucket in p.histogram:
            assert "label" in bucket
            assert "count" in bucket
            assert "pct" in bucket

    def test_smart_salary_buckets(self):
        vals = [30_000, 55_000, 85_000, 120_000, 200_000]
        p = self._profile(vals, field_path="annual_salary")
        labels = [b["label"] for b in p.histogram]
        assert any("$" in lbl for lbl in labels)

    def test_smart_age_buckets(self):
        vals = [5, 25, 42, 67]
        p = self._profile(vals, field_path="customer_age")
        labels = [b["label"] for b in p.histogram]
        assert any("18" in lbl or "65" in lbl for lbl in labels)

    def test_smart_amount_buckets(self):
        vals = [5, 100, 750, 3_000]
        p = self._profile(vals, field_path="payment_amount")
        labels = [b["label"] for b in p.histogram]
        assert any("$" in lbl for lbl in labels)

    def test_smart_score_buckets(self):
        vals = [0.1, 0.35, 0.65, 0.9]
        p = self._profile(vals, field_path="confidence_score")
        labels = [b["label"] for b in p.histogram]
        assert any("0.2" in lbl or "0.4" in lbl for lbl in labels)

    def test_currency_string_values_parsed(self):
        p = self._profile(["$1,000", "$2,500", "$500"], field_path="price")
        assert p.field_type == "numeric"
        assert p.stat_max == pytest.approx(2500.0)

    def test_all_nulls(self):
        p = self._profile([None, None, None])
        assert p.null_rate == 1.0
        assert p.stat_min is None
        assert p.histogram == []

    def test_single_value(self):
        p = self._profile([42])
        assert p.stat_min == 42.0
        assert p.stat_max == 42.0
        assert p.stat_stddev == 0.0


# ── profile_field() — categorical ─────────────────────────────────────────────

class TestProfileCategorical:
    def _profile(self, values, tag="LOCATION"):
        return compute.profile_field("orders", "country", tag, values, "2026-01-01T00:00:00Z")

    def test_histogram_frequency(self):
        values = ["US"] * 5 + ["CA"] * 3 + ["UK"] * 2
        p = self._profile(values)
        assert p.field_type == "categorical"
        top = p.histogram[0]
        assert top["label"] == "US"
        assert top["count"] == 5

    def test_top_values_exposed_for_low_sensitivity(self):
        values = ["US"] * 5 + ["CA"] * 3
        p = self._profile(values, tag="LOCATION")  # LOW
        assert len(p.top_values) > 0

    def test_top_values_hidden_for_high_sensitivity(self):
        values = ["John", "Alice", "Bob"] * 5
        p = self._profile(values, tag="PII")
        assert p.top_values == []

    def test_top_values_hidden_for_pci(self):
        values = ["Visa", "MC", "Amex"] * 5
        p = self._profile(values, tag="PCI")
        assert p.top_values == []

    def test_histogram_capped_at_15(self):
        values = [str(i) for i in range(30)] * 2  # 30 distinct values, but unique ratio > 30% ...
        # Force categorical by using a small sample (≤20 rule won't apply here — 60 total)
        # Use repeated values to keep unique ratio below 30%
        values = [str(i % 20) for i in range(100)]
        p = self._profile(values)
        assert len(p.histogram) <= 15


# ── profile_field() — boolean ──────────────────────────────────────────────────

class TestProfileBoolean:
    def _profile(self, values):
        return compute.profile_field("events", "opted_in", "LOW", values, "2026-01-01T00:00:00Z")

    def test_true_false_histogram(self):
        values = ["true"] * 7 + ["false"] * 3
        p = self._profile(values)
        assert p.field_type == "boolean"
        labels = {b["label"] for b in p.histogram}
        assert "true" in labels or "false" in labels

    def test_yes_no_boolean(self):
        values = ["yes"] * 4 + ["no"] * 6
        p = self._profile(values)
        assert p.field_type == "boolean"


# ── profile_field() — freetext ─────────────────────────────────────────────────

class TestProfileFreetext:
    def _profile(self, values):
        return compute.profile_field("records", "clinical_notes", "PHI", values, "2026-01-01T00:00:00Z")

    def test_freetext_detected_for_phi(self):
        p = self._profile(["Patient has mild fever and cough"])
        assert p.field_type == "freetext"

    def test_word_count_stats(self):
        # 3 texts with 4, 8, 12 words
        values = [
            "one two three four",
            "one two three four five six seven eight",
            " ".join(["word"] * 12),
        ]
        p = self._profile(values)
        assert p.stat_min == 4.0
        assert p.stat_max == 12.0
        assert p.stat_mean is not None

    def test_histogram_not_empty_for_freetext(self):
        values = ["word " * n for n in range(1, 20)]
        p = self._profile(values)
        assert len(p.histogram) > 0


# ── profile_field() — string ───────────────────────────────────────────────────

class TestProfileString:
    def _profile(self, values):
        return compute.profile_field("users", "email", "PII", values, "2026-01-01T00:00:00Z")

    def test_string_length_stats(self):
        values = [f"user{i}@example.com" for i in range(50)]
        p = self._profile(values)
        assert p.field_type == "string"
        assert p.stat_min is not None
        assert p.stat_max is not None
        assert p.stat_mean is not None

    def test_string_histogram_is_length_distribution(self):
        # 30 distinct values keeps unique ratio > 30% and len > 20 → "string" type
        values = [f"user{i}@example.com" for i in range(30)]
        p = self._profile(values)
        assert p.field_type == "string"
        assert len(p.histogram) > 0
        # Length histogram labels are numeric ranges (e.g. "10–19") or digits
        assert all("–" in b["label"] or b["label"].isdigit() for b in p.histogram)


# ── privacy invariant ──────────────────────────────────────────────────────────

class TestPrivacyInvariant:
    """Raw values must never surface in the profile output."""

    @pytest.mark.parametrize("tag", ["PII", "PHI", "PCI", "CREDENTIALS", "GOVERNMENT_ID", "BIOMETRIC", "GENETIC"])
    def test_high_sensitivity_no_top_values(self, tag):
        values = ["Alice", "Bob", "Charlie"] * 10
        p = compute.profile_field("t", "name", tag, values, "2026-01-01T00:00:00Z")
        assert p.top_values == [], f"{tag} should not expose top_values"

    def test_low_sensitivity_exposes_top_values(self):
        values = ["US"] * 5 + ["CA"] * 3
        p = compute.profile_field("t", "country", "LOCATION", values, "2026-01-01T00:00:00Z")
        assert len(p.top_values) > 0

    def test_medium_sensitivity_exposes_top_values(self):
        values = ["plan_a"] * 5 + ["plan_b"] * 3
        p = compute.profile_field("t", "plan_tier", "FINANCIAL", values, "2026-01-01T00:00:00Z")
        assert len(p.top_values) > 0


# ── to_dict() ──────────────────────────────────────────────────────────────────

class TestToDict:
    def test_to_dict_contains_all_fields(self):
        p = compute.profile_field("t", "score", "LOW", [1, 2, 3], "2026-01-01T00:00:00Z")
        d = p.to_dict()
        for key in ("topic", "field_path", "tag", "sensitivity", "field_type",
                    "sample_size", "null_count", "null_rate", "scanned_at",
                    "histogram", "top_values"):
            assert key in d, f"Missing key: {key}"

    def test_to_dict_no_raw_values(self):
        p = compute.profile_field("t", "email", "PII", ["a@b.com", "c@d.com"], "2026-01-01T00:00:00Z")
        d = p.to_dict()
        assert "values" not in d
        assert "raw_values" not in d
