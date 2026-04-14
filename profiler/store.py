"""
SQLite persistence for field profiles.

One row per (topic, field_path) — always replaced with the latest scan.
Raw values are never stored; only aggregate statistics and histogram bucket counts.
"""

import aiosqlite
import json
import os
from datetime import datetime, timezone

DB_PATH = os.getenv("PROFILER_DB_PATH", "/tmp/auto-profiler/profiles.db")

_CREATE = """
CREATE TABLE IF NOT EXISTS profiles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    topic           TEXT    NOT NULL,
    field_path      TEXT    NOT NULL,
    tag             TEXT    NOT NULL,
    sensitivity     TEXT    NOT NULL,
    field_type      TEXT    NOT NULL,
    sample_size     INTEGER NOT NULL,
    null_count      INTEGER NOT NULL,
    null_rate       REAL    NOT NULL,
    stat_min        REAL,
    stat_max        REAL,
    stat_mean       REAL,
    stat_median     REAL,
    stat_stddev     REAL,
    stat_p25        REAL,
    stat_p75        REAL,
    stat_p95        REAL,
    stat_zero_rate  REAL,
    histogram_json  TEXT    NOT NULL DEFAULT '[]',
    top_values_json TEXT    NOT NULL DEFAULT '[]',
    ai_summary      TEXT,
    scanned_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL,
    UNIQUE(topic, field_path)
)
"""

_UPSERT = """
INSERT INTO profiles (
    topic, field_path, tag, sensitivity, field_type,
    sample_size, null_count, null_rate,
    stat_min, stat_max, stat_mean, stat_median, stat_stddev,
    stat_p25, stat_p75, stat_p95, stat_zero_rate,
    histogram_json, top_values_json, ai_summary,
    scanned_at, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(topic, field_path) DO UPDATE SET
    tag=excluded.tag,
    sensitivity=excluded.sensitivity,
    field_type=excluded.field_type,
    sample_size=excluded.sample_size,
    null_count=excluded.null_count,
    null_rate=excluded.null_rate,
    stat_min=excluded.stat_min,
    stat_max=excluded.stat_max,
    stat_mean=excluded.stat_mean,
    stat_median=excluded.stat_median,
    stat_stddev=excluded.stat_stddev,
    stat_p25=excluded.stat_p25,
    stat_p75=excluded.stat_p75,
    stat_p95=excluded.stat_p95,
    stat_zero_rate=excluded.stat_zero_rate,
    histogram_json=excluded.histogram_json,
    top_values_json=excluded.top_values_json,
    ai_summary=excluded.ai_summary,
    scanned_at=excluded.scanned_at,
    updated_at=excluded.updated_at
"""


async def init_db(db_path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    async with aiosqlite.connect(db_path) as db:
        await db.execute(_CREATE)
        await db.commit()


async def upsert_profile(profile: dict, db_path: str = DB_PATH) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiosqlite.connect(db_path) as db:
        await db.execute(_UPSERT, (
            profile["topic"],
            profile["field_path"],
            profile["tag"],
            profile["sensitivity"],
            profile["field_type"],
            profile["sample_size"],
            profile["null_count"],
            profile["null_rate"],
            profile.get("stat_min"),
            profile.get("stat_max"),
            profile.get("stat_mean"),
            profile.get("stat_median"),
            profile.get("stat_stddev"),
            profile.get("stat_p25"),
            profile.get("stat_p75"),
            profile.get("stat_p95"),
            profile.get("stat_zero_rate"),
            json.dumps(profile.get("histogram", [])),
            json.dumps(profile.get("top_values", [])),
            profile.get("ai_summary"),
            profile["scanned_at"],
            now,
        ))
        await db.commit()


async def get_profiles(topic: str, db_path: str = DB_PATH) -> list:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM profiles WHERE topic=? ORDER BY sensitivity, field_path",
            (topic,),
        ) as cur:
            rows = await cur.fetchall()
    result = []
    for row in rows:
        d = dict(row)
        d["histogram"]  = json.loads(d.pop("histogram_json",  "[]"))
        d["top_values"] = json.loads(d.pop("top_values_json", "[]"))
        result.append(d)
    return result


async def get_topics(db_path: str = DB_PATH) -> list:
    async with aiosqlite.connect(db_path) as db:
        async with db.execute(
            "SELECT DISTINCT topic FROM profiles ORDER BY topic"
        ) as cur:
            rows = await cur.fetchall()
    return [row[0] for row in rows]
