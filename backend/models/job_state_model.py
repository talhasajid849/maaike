from __future__ import annotations

import json
from typing import Any

from config.database import get_db


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def save_xlsx_job(job_id: str, job: dict) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT INTO xlsx_jobs (
                job_id, file_id, status, source, sleep_sec, total, done, found,
                start_item, start_index, initial_found,
                stop_requested, auto_stopped, error,
                wines_json, results_json, log_json,
                template_bytes, output_bytes, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(job_id) DO UPDATE SET
                file_id=excluded.file_id,
                status=excluded.status,
                source=excluded.source,
                sleep_sec=excluded.sleep_sec,
                total=excluded.total,
                done=excluded.done,
                found=excluded.found,
                start_item=excluded.start_item,
                start_index=excluded.start_index,
                initial_found=excluded.initial_found,
                stop_requested=excluded.stop_requested,
                auto_stopped=excluded.auto_stopped,
                error=excluded.error,
                wines_json=excluded.wines_json,
                results_json=excluded.results_json,
                log_json=excluded.log_json,
                template_bytes=excluded.template_bytes,
                output_bytes=excluded.output_bytes,
                updated_at=datetime('now')
        """, (
            job_id,
            job.get("file_id"),
            job.get("status"),
            job.get("source"),
            float(job.get("sleep_sec") or 0.0),
            int(job.get("total") or 0),
            int(job.get("done") or 0),
            int(job.get("found") or 0),
            int(job.get("start_item") or 1),
            int(job.get("start_index") or 0),
            int(job.get("initial_found") or 0),
            1 if job.get("stop_requested") else 0,
            1 if job.get("auto_stopped") else 0,
            job.get("error"),
            _json_dumps(job.get("wines") or []),
            _json_dumps(job.get("results") or []),
            _json_dumps(job.get("log") or []),
            job.get("template_bytes"),
            job.get("output_bytes"),
        ))
        conn.commit()


def load_xlsx_job(job_id: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM xlsx_jobs WHERE job_id=?", (job_id,)).fetchone()
    if not row:
        return None
    return {
        "file_id": row["file_id"],
        "status": row["status"],
        "source": row["source"],
        "sleep_sec": float(row["sleep_sec"] or 0.0),
        "total": int(row["total"] or 0),
        "done": int(row["done"] or 0),
        "found": int(row["found"] or 0),
        "start_item": int(row["start_item"] or 1),
        "start_index": int(row["start_index"] or 0),
        "initial_found": int(row["initial_found"] or 0),
        "stop_requested": bool(row["stop_requested"]),
        "auto_stopped": bool(row["auto_stopped"]),
        "error": row["error"],
        "wines": _json_loads(row["wines_json"], []),
        "results": _json_loads(row["results_json"], []),
        "log": _json_loads(row["log_json"], []),
        "template_bytes": row["template_bytes"],
        "output_bytes": row["output_bytes"],
    }


def list_xlsx_jobs(statuses: list[str] | None = None, file_id: str | None = None) -> list[dict]:
    with get_db() as conn:
        conds = []
        params = []
        if statuses:
            placeholders = ",".join("?" * len(statuses))
            conds.append(f"status IN ({placeholders})")
            params.extend(statuses)
        if file_id:
            conds.append("file_id=?")
            params.append(file_id)
        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        rows = conn.execute(
            f"""
            SELECT job_id, file_id, status, source, sleep_sec, total, done, found,
                   start_item, auto_stopped, error, created_at, updated_at
            FROM xlsx_jobs
            {where}
            ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def save_enrich_snapshot(snapshot: dict) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT INTO app_state (state_key, state_json, updated_at)
            VALUES ('enrich_batch', ?, datetime('now'))
            ON CONFLICT(state_key) DO UPDATE SET
                state_json=excluded.state_json,
                updated_at=datetime('now')
        """, (_json_dumps(snapshot),))
        conn.commit()


def load_enrich_snapshot() -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT state_json FROM app_state WHERE state_key='enrich_batch'"
        ).fetchone()
    if not row:
        return None
    return _json_loads(row["state_json"], None)
