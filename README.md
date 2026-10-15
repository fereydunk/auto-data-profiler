# Auto Data Profiler

Statistical, privacy-aware profiling of field-level data flowing through Kafka topics on Confluent Cloud.

Where [`auto-data-classifier`](https://github.com/fereydunk/auto-data-classifier) answers *"what kind of data is this field?"* (PII, PHI, PCI, …), this project answers *"what does the data in this field actually look like?"* — type, null rate, distribution, cardinality, ranges — without ever persisting the raw values themselves.

## Relationship to Auto Data Classifier

This repo started as a fork of `auto-data-classifier` and still runs its full classification stack (`classifier-service`, `review-api`, `kafka-pipeline`, `flink-scanner`) unmodified, because profiling needs a tag (and its sensitivity level) before it can decide what's safe to surface about a field. The classifier stack is the input layer here, not the point of the project.

The actual profiling work lives in one new service: **`profiler/`** (port 8002). Everything else in this repo exists to feed it classified field values.

## Who is this for?

Anyone who has already tagged sensitive fields (via the classifier) and now wants to understand the data itself before building downstream pipelines, data contracts, or quality checks on it:

| Question | Answered by |
|---|---|
| "Is this field ever null? How often?" | `null_rate` |
| "Is `annual_income` actually numeric, and what's the range?" | field-type detection + min/max/mean/percentiles |
| "What does the distribution of `order_amount` look like?" | histogram with semantic bucketing |
| "Is this PII field high-cardinality (free text) or a fixed set of categories?" | field-type: `freetext` vs `categorical` |
| "Can I get a plain-English summary of this field for a data catalog entry?" | AI-generated `ai_summary` |

## How it works

```
Source topic (raw Kafka messages)
        │
        ▼
kafka-pipeline
  ├─ Deserialize (Avro via Schema Registry, or plain JSON)
  ├─ POST /classify → classifier-service   (reused as-is — see auto-data-classifier)
  ├─ Route enriched message → classified-messages / classified-safe / classification-audit
  ├─ POST recommendations → review-api     (tag approval workflow, reused as-is)
  └─ Accumulate raw field values per batch → POST /profiles/compute → profiler
                                                                          │
profiler (port 8002)  ────────────────────────────────────────────────────┘
  ├─ Detect field type per batch: numeric | boolean | categorical | freetext | string
  ├─ Compute stats: null rate, min/max/mean/median/stddev, p25/p75/p95, zero rate
  ├─ Build a histogram (semantic buckets for salary/age/amount/score/count fields,
  │    top-15 for categorical, length/word-count buckets for text)
  ├─ Gate `top_values` on sensitivity — never surfaced for HIGH tags
  │    (PII, PHI, PCI, CREDENTIALS, GOVERNMENT_ID, BIOMETRIC, GENETIC)
  ├─ Generate a 1-2 sentence AI summary via Claude (Anthropic SDK) from the
  │    aggregate stats only — raw values are never sent to the model
  ├─ Upsert one row per (topic, field_path) into SQLite — latest scan always wins
  └─ Serve GET /dashboard — a Chart.js UI to browse profiles per topic
```

The Flink SQL scanner (`flink-scanner/`) and its `apply_tags.py` review flow are also present, but they only apply *classification* tags to the Schema Registry schema — they do not feed the profiler. Only the streaming `kafka-pipeline` and `e2e/local_scanner.py` currently post to `/profiles/compute`.

> **Privacy invariant:** raw field values are used only in-memory during `compute.profile_field()` and the AI prompt build. They are never written to SQLite and never returned by any endpoint — only aggregate statistics, histogram bucket counts, and (for LOW/MEDIUM sensitivity fields only) top value counts survive.

## Field type detection

| Type | Rule |
|---|---|
| `numeric` | ≥90% of non-null sampled values parse as float |
| `boolean` | all non-null values are in `{true, false, 0, 1, yes, no}` |
| `freetext` | tag is `PHI`, or average word count ≥ 5 across the first 20 samples |
| `categorical` | unique-value ratio < 30%, or ≤ 20 total samples |
| `string` | everything else (high-cardinality text — profiled by character-length distribution) |

Sensitivity (`HIGH` / `MEDIUM` / `LOW`) is derived directly from the classification tag and controls whether `top_values` are ever included in a profile or an AI summary prompt.

## Services

| Service | Port | Purpose |
|---|---|---|
| `classifier-service` | 8000 | FastAPI — three-layer field classification (from `auto-data-classifier`, reused unmodified) |
| `review-api` | 8001 | FastAPI — human-in-the-loop tag review, Stream Catalog tagging (reused unmodified) |
| `profiler` | 8002 | FastAPI — **the profiling engine**: stats, histograms, AI summaries, dashboard |
| `kafka-pipeline` | — | Confluent Cloud consumer/producer; classifies, then batches raw values to `profiler` |

## Repository layout

```
auto-data-profiler/
├── profiler/                    Statistical profiling engine (NEW — the point of this repo)
│   ├── main.py                  FastAPI app: /health, /dashboard, /topics, /profiles, /profiles/compute
│   ├── compute.py               Field-type detection, stats, histogram bucketing (FieldProfile)
│   ├── summarize.py             AI summary generation via Anthropic SDK (stats-only prompt)
│   ├── store.py                 SQLite persistence — one row per (topic, field_path)
│   └── templates/dashboard.html Chart.js UI for browsing profiles per topic
│
├── classifier-service/          Three-layer classification engine (from auto-data-classifier)
├── review-api/                  Human-in-the-loop tag review + Stream Catalog tagging
├── kafka-pipeline/               Consumer/producer — classifies, then feeds profiler in batches
│   └── pipeline.py               accumulate_profiler() / post_profiler_batch()
├── flink-scanner/                Confluent Cloud Flink SQL UDFs — classification only, not wired to profiler
├── flink-sql/                    Flink SQL routing queries
│
├── e2e/                          End-to-end test tooling
│   └── local_scanner.py          Fallback scanner — classifies + posts to profiler locally
│
├── tests/                        Unit / integration tests, incl. test_profiler_api.py,
│                                 test_profiler_compute.py, test_profiler_summarize.py,
│                                 test_pipeline_profiler.py
├── docs/                         NOTE: currently describe the inherited classification engine
│                                 only — profiler-specific architecture docs don't exist yet.
└── docker-compose.yml            classifier + review-api + kafka-pipeline + profiler
```

## Quick start

### Run the profiler locally (no Docker)

```bash
pip install -r profiler/requirements.txt
export ANTHROPIC_API_KEY=sk-ant-...   # optional — omit to disable AI summaries
export PROFILER_DB_PATH=/tmp/auto-profiler/profiles.db

cd profiler
uvicorn main:app --port 8002

# Dashboard
open http://localhost:8002/dashboard

# Feed it a profile directly
curl -s -X POST http://localhost:8002/profiles/compute \
  -H "Content-Type: application/json" \
  -d '{"topic":"raw-messages","scanned_at":"2026-09-04T00:00:00Z",
       "fields":{"annual_income":{"tag":"FINANCIAL","values":[52000,61000,null,73500,48000]}}}' \
  | python3 -m json.tool
```

### Run the full stack with Docker Compose

```bash
cp .env.example .env   # fill in Confluent credentials + ANTHROPIC_API_KEY
docker-compose up
```

This starts `classifier` (8000), `review-api` (8001), `profiler` (8002), and `kafka-pipeline`, which classifies every message on `SOURCE_TOPIC` and streams raw field values into the profiler in batches.

### End-to-end, without Confluent credentials

```bash
bash e2e/start_classifier.sh                     # terminal 1 — classifier on :8000
cd profiler && uvicorn main:app --port 8002      # terminal 2 — profiler on :8002

python e2e/local_scanner.py \
  --bootstrap $KAFKA_BOOTSTRAP --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --source-topic raw-messages --classifier-url http://localhost:8000 \
  --profiler-url http://localhost:8002 --count 200
```

### Run tests

```bash
pytest tests/ -q
```

## Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Default | Purpose |
|---|---|---|
| `PROFILER_URL` | `http://profiler:8002` | Profiler service endpoint (set by `kafka-pipeline` and `local_scanner.py`) |
| `PROFILER_DB_PATH` | `/tmp/auto-profiler/profiles.db` | SQLite path for stored profiles (profiler service) |
| `ANTHROPIC_API_KEY` | *(unset)* | Enables AI-generated field summaries; omit to disable silently |
| `MAX_LAYER` | `3` | Classification depth feeding the profiler's `tag` input: 1=field name, 2=+regex, 3=+AI |
| `CLASSIFIER_URL` | `http://localhost:8000` | Classifier service endpoint (pipeline) |
| `REVIEW_API_URL` | `http://localhost:8001` | Review API endpoint (pipeline) |
| `SOURCE_TOPIC` | `raw-messages` | Kafka topic to consume |
| `BATCH_SIZE` | `50` | Messages per commit batch, also the profiler POST batch size |
| `MAX_CONCURRENT` | `10` | Concurrent classification requests |
| `CLASSIFIER_TIMEOUT_S` | `5.0` | Per-request timeout in seconds |

## Docs

- [Architecture](docs/ARCHITECTURE.md) — component design and data flows for both the inherited classification engine and the profiler (field-type detection, statistics, AI summaries, storage)
- [Taxonomy](docs/TAXONOMY.md) — the 11 classification tags, plus the profiler's own tag → sensitivity mapping that gates `top_values`
- [Testing](docs/TESTING.md) — unit tests (353 total: 222 classifier + 131 profiler) and an end-to-end guide for both paths
- [Tuning](docs/TUNING.md) — performance and configuration, including profiler batching, sensitivity, and AI summary cost/deployment notes
