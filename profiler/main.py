"""
Auto Data Profiler — FastAPI service (port 8002)

Endpoints:
  GET  /health                     — liveness check
  GET  /dashboard                  — Chart.js web UI
  GET  /topics                     — list profiled topics
  GET  /profiles?topic=<topic>     — get all profiles for a topic
  POST /profiles/compute           — compute + store profiles from raw field values
  POST /profiles                   — store a pre-computed profile (batch or single)
"""

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).parent))
import compute
import store
import summarize

DB_PATH = os.getenv("PROFILER_DB_PATH", "/tmp/auto-profiler/profiles.db")
_DASHBOARD_HTML = (Path(__file__).parent / "templates" / "dashboard.html").read_text()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await store.init_db(DB_PATH)
    yield


app = FastAPI(title="Auto Data Profiler", version="1.0.0", lifespan=lifespan)


# ── Pydantic models ────────────────────────────────────────────────────────────

class FieldInput(BaseModel):
    """A single field's values + classification tag for server-side profiling."""
    tag: str
    values: list[Any]


class ComputeRequest(BaseModel):
    """POST /profiles/compute — send raw field values, get profiles computed + stored."""
    topic: str
    scanned_at: str
    fields: dict[str, FieldInput]  # field_path → {tag, values}


class ProfileIn(BaseModel):
    """Pre-computed profile — POST /profiles."""
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
    histogram: list = []
    top_values: list = []
    ai_summary: Optional[str] = None


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return _DASHBOARD_HTML


@app.get("/topics")
async def list_topics():
    return await store.get_topics(DB_PATH)


@app.get("/profiles")
async def list_profiles(topic: str):
    profiles = await store.get_profiles(topic, DB_PATH)
    if not profiles:
        return []
    return profiles


@app.post("/profiles/compute", status_code=201)
async def compute_and_store(req: ComputeRequest):
    """
    Main profiling endpoint — called by local_scanner.py after classification.

    Accepts raw field values, computes statistics server-side, stores results.
    Raw values are used only during computation and never persisted.
    """
    computed = []
    for field_path, field_input in req.fields.items():
        profile = compute.profile_field(
            topic=req.topic,
            field_path=field_path,
            tag=field_input.tag,
            raw_values=field_input.values,
            scanned_at=req.scanned_at,
        )
        profile.ai_summary = await summarize.generate_summary(profile.to_dict())
        await store.upsert_profile(profile.to_dict(), DB_PATH)
        computed.append(field_path)

    return {"status": "ok", "profiled": len(computed), "fields": computed}


@app.post("/profiles", status_code=201)
async def store_profile(profile: ProfileIn):
    """Store a pre-computed profile (e.g. from an external profiler)."""
    await store.upsert_profile(profile.model_dump(), DB_PATH)
    return {"status": "ok"}
