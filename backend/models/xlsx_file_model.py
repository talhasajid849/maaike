from __future__ import annotations

from config.database import get_db


def create_xlsx_file(record: dict) -> None:
    with get_db() as conn:
        conn.execute("""
            INSERT INTO xlsx_files (
                file_id, original_name, stored_name, original_path, output_path,
                source, status, size_bytes, total_rows, prefilled_rows,
                done_rows, found_rows, active_job_id, last_job_id, last_error,
                created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
        """, (
            record["file_id"],
            record["original_name"],
            record["stored_name"],
            record["original_path"],
            record.get("output_path"),
            record.get("source"),
            record.get("status") or "pending",
            int(record.get("size_bytes") or 0),
            int(record.get("total_rows") or 0),
            int(record.get("prefilled_rows") or 0),
            int(record.get("done_rows") or 0),
            int(record.get("found_rows") or 0),
            record.get("active_job_id"),
            record.get("last_job_id"),
            record.get("last_error"),
        ))
        conn.commit()


def update_xlsx_file(file_id: str, patch: dict) -> None:
    if not patch:
        return
    fields = []
    params = []
    for key, value in patch.items():
        fields.append(f"{key}=?")
        params.append(value)
    params.extend([file_id])
    with get_db() as conn:
        conn.execute(
            f"UPDATE xlsx_files SET {', '.join(fields)}, updated_at=datetime('now') WHERE file_id=?",
            params,
        )
        conn.commit()


def get_xlsx_file(file_id: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM xlsx_files WHERE file_id=?", (file_id,)).fetchone()
    return dict(row) if row else None


def list_xlsx_files() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("""
            SELECT *
            FROM xlsx_files
            ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
        """).fetchall()
    return [dict(row) for row in rows]


def delete_xlsx_file_record(file_id: str) -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM xlsx_files WHERE file_id=?", (file_id,))
        conn.execute("DELETE FROM xlsx_jobs WHERE file_id=?", (file_id,))
        conn.commit()
