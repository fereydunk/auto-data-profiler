"""
Statistical profiling engine.

Takes a field name, classification tag, and list of raw sampled values.
Returns a FieldProfile with aggregate statistics and histogram bucket counts.

PRIVACY INVARIANT: raw values are never persisted or returned.
Only aggregate bucket counts, percentiles, and derived statistics survive.

Field type detection:
  numeric     — 90%+ of non-null values are parseable as float
  boolean     — all non-null values are in {true, false, 0, 1, yes, no}
  categorical — unique ratio < 30% (many repeated values)
  freetext    — PHI tag OR avg word count >= 4
  string      — everything else (high-cardinality text)

Histogram bucketing:
  numeric     — smart semantic buckets by field name keyword, else 8 equal-width
  categorical — top-15 values by frequency (horizontal bar chart)
  string      — string length distribution (character count buckets)
  boolean     — true/false counts
  freetext    — word count distribution
"""

import math
import statistics
from dataclasses import dataclass, field
from typing import Optional

# Tags that must never expose top_values or raw sample labels
_HIGH_SENSITIVITY = {"PII", "PHI", "PCI", "CREDENTIALS", "GOVERNMENT_ID", "BIOMETRIC", "GENETIC"}
_MEDIUM_SENSITIVITY = {"FINANCIAL", "NPI"}

# Keyword sets for smart numeric bucketing
_SALARY_KEYS = {"salary", "income", "wage", "pay", "compensation", "earning", "annual"}
_AGE_KEYS = {"age", "dob", "birth", "year"}
_AMOUNT_KEYS = {"amount", "price", "cost", "fee", "charge", "payment", "balance", "revenue", "spend"}
_SCORE_KEYS = {"score", "rating", "rank", "confidence", "probability", "pct", "percent"}
_COUNT_KEYS = {"count", "quantity", "qty", "num", "number", "total", "cnt"}


def sensitivity(tag: str) -> str:
    if tag in _HIGH_SENSITIVITY:
        return "HIGH"
    if tag in _MEDIUM_SENSITIVITY:
        return "MEDIUM"
    return "LOW"


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "").replace("%", "")
    try:
        return float(s)
    except ValueError:
        return None


def _detect_type(field_path: str, tag: str, non_null: list) -> str:
    if not non_null:
        return "string"

    sample = non_null[:100]

    # Boolean check
    bool_set = {True, False, "true", "false", "yes", "no", "0", "1", 0, 1}
    if all(str(v).lower() in {"true", "false", "yes", "no", "0", "1"} for v in sample):
        return "boolean"

    # Numeric check — 90%+ parseable
    numeric_hits = sum(1 for v in sample if _to_float(v) is not None)
    if numeric_hits / len(sample) >= 0.90:
        return "numeric"

    # Free-text — PHI or high word count
    if tag == "PHI" or any(len(str(v).split()) >= 5 for v in sample[:20]):
        return "freetext"

    # Categorical — low unique ratio
    unique_ratio = len({str(v) for v in non_null}) / len(non_null)
    if unique_ratio < 0.30 or len(non_null) <= 20:
        return "categorical"

    return "string"


def _percentile(sorted_vals: list, p: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = (p / 100.0) * (n - 1)
    lo = int(idx)
    hi = min(lo + 1, n - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)


def _smart_edges(field_path: str, min_v: float, max_v: float):
    """Return (edges, labels) for semantic histogram bucketing."""
    name = field_path.lower().replace("_", " ").replace(".", " ")

    if any(k in name for k in _SALARY_KEYS) and max_v > 1000:
        edges  = [0, 25_000, 50_000, 75_000, 100_000, 150_000, 250_000, max_v + 1]
        labels = ["$0–25K", "$25–50K", "$50–75K", "$75–100K", "$100–150K", "$150–250K", "$250K+"]
        return edges, labels

    if any(k in name for k in _AGE_KEYS) and max_v <= 130:
        edges  = [0, 18, 35, 50, 65, max_v + 1]
        labels = ["0–17", "18–34", "35–49", "50–64", "65+"]
        return edges, labels

    if any(k in name for k in _AMOUNT_KEYS) and max_v > 0:
        edges  = [0, 10, 50, 200, 500, 1_000, 5_000, max_v + 1]
        labels = ["$0–10", "$10–50", "$50–200", "$200–500", "$500–1K", "$1K–5K", "$5K+"]
        return edges, labels

    if any(k in name for k in _SCORE_KEYS) and 0 <= min_v and max_v <= 1.05:
        edges  = [0, 0.2, 0.4, 0.6, 0.8, 1.0, max_v + 0.01]
        labels = ["0–0.2", "0.2–0.4", "0.4–0.6", "0.6–0.8", "0.8–1.0", ">1.0"]
        return edges, labels

    if any(k in name for k in _COUNT_KEYS) and min_v >= 0:
        step = max(1.0, math.ceil((max_v - min_v) / 8))
        edges = [min_v + i * step for i in range(9)]
        edges[-1] = max_v + 1
        labels = [f"{int(edges[i])}–{int(edges[i+1]-1)}" for i in range(8)]
        return edges, labels

    # Generic equal-width
    span = max_v - min_v
    if span == 0:
        return [min_v, min_v + 1], [str(int(min_v))]
    step = span / 8
    edges = [min_v + i * step for i in range(9)]
    edges[-1] = max_v + 1e-9
    labels = [f"{edges[i]:.2g}–{edges[i+1]:.2g}" for i in range(8)]
    return edges, labels


def _build_numeric_histogram(field_path: str, nums: list) -> list:
    if not nums:
        return []
    min_v, max_v = min(nums), max(nums)
    edges, labels = _smart_edges(field_path, min_v, max_v)
    buckets = [0] * len(labels)
    for n in nums:
        placed = False
        for i in range(len(edges) - 1):
            if edges[i] <= n < edges[i + 1]:
                buckets[i] += 1
                placed = True
                break
        if not placed:
            buckets[-1] += 1
    total = len(nums)
    return [
        {"label": labels[i], "count": buckets[i], "pct": round(100 * buckets[i] / total, 1)}
        for i in range(len(labels))
        if buckets[i] > 0
    ]


def _build_length_histogram(values: list) -> list:
    lengths = [len(str(v)) for v in values]
    if not lengths:
        return []
    max_l = max(lengths)
    if max_l <= 20:
        edges = list(range(0, max_l + 2))
        labels = [str(i) for i in range(max_l + 1)]
    elif max_l <= 100:
        edges = [0, 10, 20, 30, 50, 75, 100, max_l + 1]
        labels = ["0–9", "10–19", "20–29", "30–49", "50–74", "75–99", "100+"]
    else:
        edges = [0, 20, 50, 100, 200, 500, max_l + 1]
        labels = ["0–19", "20–49", "50–99", "100–199", "200–499", "500+"]
    buckets = [0] * len(labels)
    for l in lengths:
        for i in range(len(edges) - 1):
            if edges[i] <= l < edges[i + 1]:
                buckets[i] += 1
                break
        else:
            buckets[-1] += 1
    total = len(lengths)
    return [
        {"label": labels[i], "count": buckets[i], "pct": round(100 * buckets[i] / total, 1)}
        for i in range(len(labels))
        if buckets[i] > 0
    ]


@dataclass
class FieldProfile:
    topic: str
    field_path: str
    tag: str
    sensitivity: str
    field_type: str
    sample_size: int
    null_count: int
    null_rate: float
    scanned_at: str
    stat_min: Optional[float] = None
    stat_max: Optional[float] = None
    stat_mean: Optional[float] = None
    stat_median: Optional[float] = None
    stat_stddev: Optional[float] = None
    stat_p25: Optional[float] = None
    stat_p75: Optional[float] = None
    stat_p95: Optional[float] = None
    stat_zero_rate: Optional[float] = None
    histogram: list = field(default_factory=list)
    top_values: list = field(default_factory=list)
    ai_summary: Optional[str] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


def profile_field(
    topic: str,
    field_path: str,
    tag: str,
    raw_values: list,
    scanned_at: str,
) -> FieldProfile:
    """
    Compute a FieldProfile from raw sampled values.
    Raw values are used only for computation — never stored or returned.
    """
    non_null = [v for v in raw_values if v is not None and v != ""]
    null_count = len(raw_values) - len(non_null)
    null_rate = round(null_count / len(raw_values), 4) if raw_values else 0.0
    sens = sensitivity(tag)
    ftype = _detect_type(field_path, tag, non_null)

    profile = FieldProfile(
        topic=topic,
        field_path=field_path,
        tag=tag,
        sensitivity=sens,
        field_type=ftype,
        sample_size=len(raw_values),
        null_count=null_count,
        null_rate=null_rate,
        scanned_at=scanned_at,
    )

    if ftype == "numeric" and non_null:
        nums = [f for v in non_null if (f := _to_float(v)) is not None]
        if nums:
            nums_sorted = sorted(nums)
            profile.stat_min    = round(min(nums), 4)
            profile.stat_max    = round(max(nums), 4)
            profile.stat_mean   = round(statistics.mean(nums), 4)
            profile.stat_median = round(statistics.median(nums), 4)
            profile.stat_stddev = round(statistics.stdev(nums), 4) if len(nums) > 1 else 0.0
            profile.stat_p25    = round(_percentile(nums_sorted, 25), 4)
            profile.stat_p75    = round(_percentile(nums_sorted, 75), 4)
            profile.stat_p95    = round(_percentile(nums_sorted, 95), 4)
            profile.stat_zero_rate = round(sum(1 for n in nums if n == 0) / len(nums), 4)
            profile.histogram   = _build_numeric_histogram(field_path, nums)

    elif ftype in ("categorical", "boolean") and non_null:
        str_vals = [str(v).lower() if ftype == "boolean" else str(v) for v in non_null]
        counts: dict = {}
        for v in str_vals:
            counts[v] = counts.get(v, 0) + 1
        sorted_counts = sorted(counts.items(), key=lambda x: -x[1])
        total = len(str_vals)
        profile.histogram = [
            {"label": v, "count": c, "pct": round(100 * c / total, 1)}
            for v, c in sorted_counts[:15]
        ]
        # Only surface actual values for LOW / MEDIUM sensitivity
        if sens in ("LOW", "MEDIUM"):
            profile.top_values = [
                {"value": v, "count": c, "pct": round(100 * c / total, 1)}
                for v, c in sorted_counts[:10]
            ]

    elif ftype == "freetext" and non_null:
        # Word count distribution
        word_counts = [len(str(v).split()) for v in non_null]
        if word_counts:
            wc_sorted = sorted(word_counts)
            profile.stat_min    = float(min(word_counts))
            profile.stat_max    = float(max(word_counts))
            profile.stat_mean   = round(statistics.mean(word_counts), 1)
            profile.stat_median = float(statistics.median(word_counts))
            edges  = [0, 10, 25, 50, 100, 200, 500, max(word_counts) + 1]
            labels = ["0–9 words", "10–24", "25–49", "50–99", "100–199", "200–499", "500+"]
            buckets = [0] * len(labels)
            for w in word_counts:
                for i in range(len(edges) - 1):
                    if edges[i] <= w < edges[i + 1]:
                        buckets[i] += 1
                        break
                else:
                    buckets[-1] += 1
            total = len(word_counts)
            profile.histogram = [
                {"label": labels[i], "count": buckets[i], "pct": round(100 * buckets[i] / total, 1)}
                for i in range(len(labels)) if buckets[i] > 0
            ]

    elif ftype == "string" and non_null:
        lengths = [len(str(v)) for v in non_null]
        if lengths:
            profile.stat_min    = float(min(lengths))
            profile.stat_max    = float(max(lengths))
            profile.stat_mean   = round(statistics.mean(lengths), 1)
            profile.stat_median = float(statistics.median(lengths))
        profile.histogram = _build_length_histogram(non_null)

    return profile
