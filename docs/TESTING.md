# Testing

## Test suite overview

```
tests/
├── conftest.py                      sys.path setup for classifier-service + kafka-pipeline
├── test_classifier_api.py           /classify and /health endpoint tests (13 tests)
├── test_field_name_recognizer.py    Layer 1 — classify_field_name() + is_free_text() (74 tests)
├── test_taxonomy.py                 DataTag enum + tag_entity() (43 tests)
├── test_phi_recognizers.py          PHI pattern recognisers (8 tests)
├── test_pci_recognizers.py          PCI pattern recognisers (12 tests)
├── test_financial_recognizers.py    FINANCIAL pattern recognisers (7 tests)
├── test_credentials_recognizers.py  CREDENTIALS pattern recognisers (12 tests)
├── test_catalog_tagger.py           Stream Catalog tag application logic (23 tests)
├── test_review_api.py               Review API — create, upsert, approve, reject, bulk (23 tests)
├── test_wire_format.py              Confluent Avro wire-format deserialization (7 tests)
│
│                                    Profiler-specific (this repo's addition):
├── test_profiler_compute.py         profile_field() — type detection, stats, histograms (66 tests)
├── test_profiler_api.py             profiler FastAPI endpoints — /health, /dashboard,
│                                     /topics, /profiles, /profiles/compute (22 tests)
├── test_profiler_summarize.py       AI summary prompt building + generate_summary() (24 tests)
└── test_pipeline_profiler.py        kafka-pipeline profiler integration — _get_field_value(),
                                      accumulate_profiler(), post_profiler_batch() (19 tests)
```

**Total: 353 tests (222 inherited from the classifier engine + 131 profiler-specific). All pass.**

---

## Running the tests

```bash
# All tests
pytest tests/ -q

# Single file
pytest tests/test_classifier_api.py -v

# Specific test
pytest tests/test_field_name_recognizer.py::TestClassifyFieldName::test_camel_case_handling -v

# With coverage
pytest tests/ --cov=classifier-service --cov=profiler --cov-report=term-missing
```

`test_pipeline_profiler.py` imports `kafka-pipeline/pipeline.py`, which requires
`confluent-kafka` (pinned in `kafka-pipeline/requirements.txt`) to be installed, in addition to
`profiler/requirements.txt` for the other three profiler test files.

---

## Unit tests — key design decisions

### Mocking the analyzer (test_classifier_api.py)

The classifier service has two analyzers (`regex_analyzer`, `ai_analyzer`). Both are mocked in tests so spaCy and GLiNER never load:

```python
@pytest.fixture()
def client():
    mock_analyzer = MagicMock()
    with patch("main.build_regex_analyzer", return_value=mock_analyzer), \
         patch("main.build_ai_analyzer", return_value=mock_analyzer):
        import main as app_module
        app_module.regex_analyzer = mock_analyzer
        app_module.ai_analyzer = mock_analyzer
        with TestClient(app_module.app) as c:
            c._mock_analyzer = mock_analyzer
            yield c
```

`client._mock_analyzer.analyze.return_value = [...]` controls what the analyzer returns for each test.

### Module name collision (test_review_api.py)

Both `classifier-service/main.py` and `review-api/main.py` are named `main`. When the full suite runs, Python's module cache serves the wrong one. Fixed by:
- Not adding `review-api` to `conftest.py` sys.path
- Using `_load_review_api()` in `test_review_api.py` which removes the path, re-inserts at front, and clears `sys.modules` before importing

### SQLite in tests (test_review_api.py)

Tests use pytest's `tmp_path` fixture to create a real per-test SQLite file — **not** `:memory:`. Each `aiosqlite.connect(":memory:")` call creates a fresh empty database, so tables initialised in one call would not persist to the next.

```python
@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_recommendations.db")
```

The same pattern is used in `test_profiler_api.py` for the profiler's own SQLite store
(`PROFILER_DB_PATH` pointed at a `tmp_path` file per test).

### Mocking the Anthropic client (test_profiler_summarize.py)

`generate_summary()` accepts an optional `client` argument specifically so tests never construct a
real `AsyncAnthropic` client or make a network call. Tests pass a `MagicMock`/`AsyncMock` and
assert against `_build_prompt()`'s output directly, plus exercise the "no API key" and "package not
installed" paths by patching `os.getenv` and `sys.modules` respectively.

### Privacy-invariant tests (test_profiler_compute.py::TestPrivacyInvariant)

A dedicated test class asserts that `FieldProfile.to_dict()` and the stored profile never contain
the raw input values — only the fields defined on `FieldProfile` (stats, histogram, top_values,
ai_summary). This is the regression guard for the profiler's core privacy claim: it's a test on
the dataclass shape, not a network-level check, so it fails immediately if a future change adds a
raw-value field to `FieldProfile` without updating this test.

---

## End-to-end test guide

### Prerequisites

```bash
# Install missing dependencies (one-time)
brew install ngrok/ngrok/ngrok
.venv/bin/pip install gliner
.venv/bin/python -m spacy download en_core_web_lg
```

### Step 1 — Start the classifier service

```bash
bash e2e/start_classifier.sh
```

This installs any missing deps, starts uvicorn on port 8000, and waits for `/health` to return `ok`. GLiNER downloads its model (~500 MB) on first run — allow 60–90 seconds.

### Step 2 — Expose with ngrok

In a new terminal tab:
```bash
ngrok http 8000
# → https://xxxx.ngrok-free.app
```

### Step 3 — Run the smoke test

```bash
# Classifier only (no Confluent credentials needed)
.venv/bin/python e2e/verify_e2e.py \
  --classifier-url https://xxxx.ngrok-free.app

# Full end-to-end including Kafka round-trip
.venv/bin/python e2e/verify_e2e.py \
  --classifier-url https://xxxx.ngrok-free.app \
  --bootstrap      pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
  --api-key        KAFKA_KEY \
  --api-secret     KAFKA_SECRET \
  --topic          scanner-test
```

Expected output:
```
── Classifier service (https://xxxx.ngrok-free.app) ──
  ✓ Health OK — version 3.1.0

── /classify ──
  ✓ Layer 1 — field name 'ssn' → GOVERNMENT_ID  (layers_used=[1])
  ✓ Layer 1 — field name 'email' → PII  (layers_used=[1])
  ✓ Layer 1 — field name 'password' → CREDENTIALS  (layers_used=[1])
  ✓ Layer 1 — field name 'credit_card_number' → PCI  (layers_used=[1])
  ✓ Layer 1 — field name 'patient_id' → PHI  (layers_used=[1])
  ✓ Layer 2 — routing number in value → FINANCIAL  (layers_used=[1, 2])
  ✓ Layer 2 — regex credit card in value → PCI  (layers_used=[1, 2])
  ✓ Layer 2 — regex email in value → PII  (layers_used=[1, 2])
  ✓ Layer 3 — AI model detects PHI in free-text  (layers_used=[1, 2, 3])
  ✓ max_layer=1 skips regex/AI → (no tags, as expected)
  ✓ nested fields flattened → PII  (layers_used=[1])

── Result: 11 passed, 0 failed
```

### Layer 2 and Layer 3 test case rationale

Two verify_e2e.py test cases were updated after finding classifier limitations during e2e validation:

**Layer 2 — SSN in value (changed to routing number)**

The original test used `{"note": "SSN is 123-45-6789"}` and expected `GOVERNMENT_ID`. In practice Presidio's `UsSsnRecognizer` does not reliably detect SSNs embedded in mixed-text when the field name provides no SSN context — the `123-45-6789` pattern is ambiguous with `DATE_TIME` (e.g. Dec 45 6789 in some locale formats). The test was replaced with a routing number: `{"ref": "routing number 026009593"}` expecting `FINANCIAL`. The explicit label "routing number" before the digits gives Presidio's `USRoutingNumberRecognizer` the context it needs for a confident match.

**Layer 3 — AI GOVERNMENT_ID (changed to PHI)**

The original test used `{"comment": "My name is John Smith and my SSN is 123-45-6789"}` and expected `GOVERNMENT_ID`. The GLiNER model returns a `PERSON` entity (tag `PII`) from the name, and the SSN pattern in free-text is not reliably mapped to `GOVERNMENT_ID` without field-name context. The test was replaced with `{"freetext": "Patient diagnosed with Type 2 Diabetes, prescribed Metformin 500mg"}` expecting `PHI`. GLiNER reliably detects medical conditions and medications as PHI in free-text, which is the primary use case for Layer 3 in this classifier.

### Step 3b — Run the kafka-pipeline against Confluent Cloud

Start the review-api and pipeline with Mac-appropriate settings:

```bash
# Terminal 1 — start the classifier (already running from Step 1)
# Terminal 2 — start the review-api
cd review-api
DB_PATH=/tmp/auto-classifier-data/recommendations.db \
  .venv/bin/uvicorn main:app --port 8001

# Terminal 3 — start the pipeline with Mac settings
MAX_CONCURRENT=3 CLASSIFIER_TIMEOUT_S=15 \
  .venv/bin/python kafka-pipeline/pipeline.py
```

The pipeline will idle-flush partial batches when the topic quiets down, so all messages are committed within ~1 second of arrival even when the batch size is not reached.

### Step 4 — Produce test data to Confluent Cloud

```bash
.venv/bin/python e2e/produce_test_data.py \
  --bootstrap  pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
  --api-key    KAFKA_KEY \
  --api-secret KAFKA_SECRET \
  --topic      scanner-test \
  --count      200
```

Produces 200 JSON messages covering all 11 tag types including nested structures, free-text comment fields, and mixed-type messages (e.g. e-commerce orders with PII + PCI).

### Step 5 — Run the Flink SQL scanner

> **Prerequisite — USING CONNECTIONS must be enabled for your org:**
> Confluent Cloud Flink internet egress for UDFs requires the **USING CONNECTIONS** feature to be
> enabled for your Confluent Cloud organisation by the Confluent account team. Without it,
> `classify_fields()` cannot reach external URLs. Verify with:
> `confluent flink connection list --environment <env-id> --cloud aws --region us-west-2`
> If the `classifier-service` connection is present, the feature is active.

```bash
# 1. Build the UDF JAR
bash flink-scanner/scripts/build.sh

# 2. Register all three UDFs in Confluent Cloud (once per environment)
bash flink-scanner/scripts/register.sh
# Registers: classify_fields() (USING CONNECTIONS classifier-service), schema_watcher(), apply_tag()
# (requires CONFLUENT_ENVIRONMENT and CONFLUENT_COMPUTE_POOL set, or export before running)

# 3. Fill in flink-scanner/scan.env with your credentials and schedule
vim flink-scanner/scan.env
#   SCAN_INTERVAL_MINUTES=2    ← short for testing; use 60 for production
#   SAMPLE_WINDOW_MINUTES=1    ← how much data per scan
#   CLASSIFIER_URL             ← must match the classifier-service connection endpoint
#   SOURCE_TOPIC, SR_URL/KEY/SECRET, KAFKA_BOOTSTRAP/KEY/SECRET

# 4. Start all three Flink statements (run once — they run continuously)
bash flink-scanner/scripts/start_scan.sh
# Starts:
#   {topic}-scan-trigger-scheduled  RUNNING — TUMBLE window, fires every N min
#   {topic}-scan-trigger-schema     RUNNING — SchemaWatcherUDF, reacts to SR changes
#   {topic}-scan-driver             RUNNING — interval join + classify_fields() via USING CONNECTIONS

# 5. Trigger a manual scan (inserts one row; scan driver reacts immediately)
bash flink-scanner/scripts/start_scan.sh --now
# Fire a second trigger ~60s later to advance the watermark and unblock the interval join:
bash flink-scanner/scripts/start_scan.sh --now

# 6. Check status
bash flink-scanner/scripts/start_scan.sh --status

# Stop all scanner statements
bash flink-scanner/scripts/start_scan.sh --stop
```

Expected results in `{topic}-scan-results` after two manual triggers over 200 test messages:
```
~1,874 rows — one per (field_path, tag) detection
All 11 tags represented: PII, PHI, PCI, CREDENTIALS, FINANCIAL, GOVERNMENT_ID,
                          BIOMETRIC, GENETIC, NPI, LOCATION, MINOR
```

### Step 5b — Classify with local_scanner.py (fallback path)

Use `local_scanner.py` only if USING CONNECTIONS is **not** enabled for your org.
It replicates Statement C's logic locally, writing to the same `{topic}-scan-results` topic.
`apply_tags.py` handles results from both paths identically (Avro from Flink, JSON from local scanner).

```bash
source flink-scanner/scan.env

# Produce fresh test data so messages are within the sample window
python e2e/produce_test_data.py \
  --bootstrap  $KAFKA_BOOTSTRAP \
  --api-key    $KAFKA_KEY \
  --api-secret $KAFKA_SECRET \
  --count      200

# Run local scanner — reads Kafka, calls local classifier, writes results to Kafka
python e2e/local_scanner.py \
  --bootstrap       $KAFKA_BOOTSTRAP \
  --api-key         $KAFKA_KEY \
  --api-secret      $KAFKA_SECRET \
  --classifier-url  http://localhost:8000 \
  --max-layer       3 \
  --count           200

# Expected output:
#   200/200 messages processed  (1987 tag detections so far)
#   Done. Processed 200 messages → 1987 tag detections.
#   Tag summary:
#     PII                   966
#     PHI                   188
#     PCI                   160
#     ...
#   Results written to Kafka topic: raw-messages-scan-results
```

### Step 6 — Apply tags to Schema Registry

```bash
source flink-scanner/scan.env

python flink-scanner/apply_tags.py \
  --topic     $SOURCE_TOPIC \
  --sr-url    $SR_URL --sr-key $SR_KEY --sr-secret $SR_SECRET \
  --bootstrap $KAFKA_BOOTSTRAP --kafka-key $KAFKA_KEY --kafka-secret $KAFKA_SECRET
# → reads latest batch from {topic}-scan-results (groups by max scanned_at)
# → skips fields already tagged in schema
# → prompts [y/n/q] for new/changed tags
# → patches schema (AVRO / JSON Schema / Protobuf) → registers one new version

# To approve all without interactive prompt:
python flink-scanner/apply_tags.py ... --yes
```

---

## Profiler end-to-end guide

The profiler is only fed by the **streaming pipeline** and `e2e/local_scanner.py` — the Flink
scanner path (Steps 5/5b above) does not touch it.

### Step A — Start the profiler service

```bash
pip install -r profiler/requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...   # optional — omit to leave ai_summary null
export PROFILER_DB_PATH=/tmp/auto-profiler/profiles.db

cd profiler
uvicorn main:app --port 8002
```

### Step B — Feed it via local_scanner.py (no Confluent Cloud pipeline required)

```bash
python e2e/local_scanner.py \
  --bootstrap       $KAFKA_BOOTSTRAP --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --source-topic    raw-messages \
  --classifier-url  http://localhost:8000 \
  --profiler-url    http://localhost:8002 \
  --max-layer       3 \
  --count           200
```

`local_scanner.py` prints `[profiler] stored profiles for N field(s)` per batch on success, or
`[warn] profiler call failed: ...` on failure — the run still completes either way since the
`/profiles/compute` POST is best-effort.

### Step C — Feed it via the live kafka-pipeline instead

```bash
PROFILER_URL=http://localhost:8002 MAX_CONCURRENT=3 CLASSIFIER_TIMEOUT_S=15 \
  python kafka-pipeline/pipeline.py
```

### Step D — Check the results

```bash
curl -s http://localhost:8002/topics | python3 -m json.tool
curl -s "http://localhost:8002/profiles?topic=raw-messages" | python3 -m json.tool
open http://localhost:8002/dashboard
```

A `HIGH`-sensitivity field (e.g. a `PII`/`PCI` tag) should show stats and a histogram but an empty
`top_values` array; a `LOW`/`MEDIUM` field (e.g. `LOCATION`, `FINANCIAL`) should show populated
`top_values`. If `ANTHROPIC_API_KEY` is set, `ai_summary` should be a non-null sentence — if it's
consistently `null` with a key set, check the model ID note in
[ARCHITECTURE.md](ARCHITECTURE.md#ai-summary-generation).

---

## What each test file covers

### test_classifier_api.py
- `/health` returns `status: ok`, `analyzer_ready: true`, `version: 3.1.0`
- Layer 1 detection from field name (`ssn`, `email`, `password`)
- `max_layer=1` skips data inspection — `analyze()` never called
- `max_layer=1` with a named field still detects (Layer 1 always runs)
- Regex results tagged as `layer=2`
- `layers_used` matches `max_layer` setting
- Clean fields return empty tags
- Nested fields are flattened
- Response includes `classifier_version`, `classified_at`, `layers_used`
- No `sensitivity_level` or `categories` in response (removed in v3.0)

### test_field_name_recognizer.py
- All 11 tags via field names (parametrised — 70+ field name cases)
- Generic names (`id`, `value`, `data`, `ref`) return no match
- Sorted by confidence descending
- camelCase handling (`creditCardNumber`, `dateOfBirth`, `socialSecurityNumber`)
- `source == "field_name"` and `layer == 1` on all results
- No duplicate entity types per field
- `is_free_text()`: known names, structured names, value word-count heuristic

### test_review_api.py (23 tests)
- Create recommendation, retrieve by ID
- Upsert keeps highest confidence for same (topic, field, tag)
- List with status/topic/tag/tier filters
- Approve → calls catalog_client, status → APPROVED
- Reject → status → REJECTED
- Bulk-approve above min_confidence threshold
- Summary counts by topic

### test_profiler_compute.py (66 tests)
- `sensitivity()` — correct HIGH/MEDIUM/LOW for every tag, unknown tag defaults LOW
- `_to_float()` — int/float passthrough, string with `$`/`,`/`%` stripped, unparseable → None
- `_detect_type()` — boolean/numeric/freetext/categorical/string boundary cases (exact 90% numeric
  threshold, exact 30% unique-ratio threshold, PHI forces freetext regardless of word count)
- Numeric profiling — min/max/mean/median/stddev/percentiles, zero_rate, semantic histogram
  bucketing per keyword group (salary/age/amount/score/count) and the equal-width fallback
- Categorical/boolean profiling — top-15 histogram, `top_values` present only for LOW/MEDIUM
- Freetext profiling — word-count distribution buckets
- String profiling — character-length distribution buckets
- `TestPrivacyInvariant` — `FieldProfile.to_dict()` never contains raw input values
- `TestToDict` — round-trips all fields including optional stats and `ai_summary`

### test_profiler_api.py (22 tests)
- `/health` returns `status: ok`
- `/dashboard` returns the HTML template
- `/topics` and `/profiles?topic=` empty-state behavior before any data is stored
- `/profiles/compute` — 201, correct profiled count, topic/field appear afterward, numeric fields
  get stats, categorical fields get a histogram, HIGH-sensitivity fields never get `top_values`,
  null rate computed correctly, upsert replaces the prior profile for the same field
- `/profiles` (pre-computed) — store and retrieve, upsert semantics, optional stats, `ai_summary`
- Sorting — `/profiles` returns all three sensitivity tiers, ordered by sensitivity then field name

### test_profiler_summarize.py (24 tests)
- `_build_prompt()` — field/topic/tag/type/sample-size/null-rate always included; numeric stats
  included only for numeric fields; `top_values` included only for LOW/MEDIUM sensitivity
- `generate_summary()` — returns the model's text block, passes a stats-only prompt (no raw
  values), returns `None` and logs a warning on any exception rather than raising
- `_get_client()` — returns `None` silently when `ANTHROPIC_API_KEY` is unset or the `anthropic`
  package is not installed

### test_pipeline_profiler.py (19 tests)
- `_get_field_value()` — direct key lookup, dot-path traversal into nested dicts, missing path
  returns `None` instead of raising
- `accumulate_profiler()` — only fields with detected entities are buffered, tag is the
  highest-confidence entity's tag, values accumulate across multiple calls for the same field,
  fields with no matching raw value are skipped
- `post_profiler_batch()` — no-op when the buffer is empty or `PROFILER_URL` is unset, POSTs the
  expected payload shape, swallows `httpx.HTTPError` without raising

---

## CI considerations

The test suite requires no network access and no GPU:
- spaCy and GLiNER are stubbed in `test_classifier_api.py`
- spaCy `en_core_web_sm` is used by the recogniser tests (installed in venv)
- GLiNER is stubbed via `sys.modules.setdefault("gliner", MagicMock())` at the top of API tests
- All SQLite operations use `tmp_path` (no persistent state between tests)

To run in CI without the large models:
```bash
pip install -r classifier-service/requirements.txt --extra-index-url https://...
# spacy download en_core_web_sm is sufficient for unit tests
python -m spacy download en_core_web_sm
pip install -r profiler/requirements.txt -r kafka-pipeline/requirements.txt
pytest tests/ -q
```

The profiler tests need no network access either: `test_profiler_summarize.py` never constructs a
real Anthropic client (see [Mocking the Anthropic client](#mocking-the-anthropic-client-test_profiler_summarizepy)
above), and `test_profiler_api.py` / `test_pipeline_profiler.py` use `tmp_path` SQLite files, so
`ANTHROPIC_API_KEY` does not need to be set in CI for the suite to pass.
