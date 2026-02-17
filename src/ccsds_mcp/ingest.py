from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pymupdf

from ccsds_mcp.db import connect_db, ensure_schema

SPACE_TAB_RE = re.compile(r"[ \t]+")
THREE_PLUS_NEWLINES_RE = re.compile(r"\n{3,}")


@dataclass(slots=True)
class IngestStats:
    discovered: int = 0
    ingested: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0


@dataclass(slots=True)
class DocumentRow:
    doc_id: int
    sha256: str


@dataclass(slots=True)
class DocumentResult:
    status: Literal["ingested", "updated", "skipped"]
    page_count: int


def discover_pdfs(pdf_dir: Path) -> list[Path]:
    pdfs: list[Path] = [
        path
        for path in pdf_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".pdf"
    ]
    pdfs.sort(key=lambda path: str(path.resolve()))
    return pdfs


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_text(text: str) -> str:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = SPACE_TAB_RE.sub(" ", normalized)
    normalized = THREE_PLUS_NEWLINES_RE.sub("\n\n", normalized)
    return normalized.strip()


def extract_pages(pdf_bytes: bytes, source_path: Path) -> list[str]:
    pages: list[str] = []
    try:
        with pymupdf.open(stream=pdf_bytes, filetype="pdf") as document:
            for page_index in range(document.page_count):
                page = document.load_page(page_index)
                pages.append(normalize_text(page.get_text("text")))
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Unable to read PDF '{source_path}': {exc}") from exc
    return pages


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_existing_document(
    conn: sqlite3.Connection, resolved_path: str
) -> DocumentRow | None:
    cursor = conn.execute(
        "SELECT doc_id, sha256 FROM documents WHERE path = ?",
        (resolved_path,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return DocumentRow(doc_id=int(row["doc_id"]), sha256=str(row["sha256"]))


def write_document(
    conn: sqlite3.Connection,
    resolved_path: str,
    filename: str,
    sha256: str,
    pages: list[str],
    existing: DocumentRow | None,
) -> Literal["ingested", "updated"]:
    ingested_at = utc_now_iso()
    status: Literal["ingested", "updated"] = "ingested"
    conn.execute("BEGIN")
    try:
        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO documents(path, filename, sha256, page_count, ingested_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (resolved_path, filename, sha256, len(pages), ingested_at),
            )
            doc_id = int(cursor.lastrowid)
        else:
            status = "updated"
            doc_id = existing.doc_id
            conn.execute(
                """
                UPDATE documents
                SET filename = ?, sha256 = ?, page_count = ?, ingested_at = ?
                WHERE doc_id = ?
                """,
                (filename, sha256, len(pages), ingested_at, doc_id),
            )
            conn.execute("DELETE FROM pages WHERE doc_id = ?", (doc_id,))

        conn.executemany(
            "INSERT INTO pages(doc_id, page_index, text) VALUES (?, ?, ?)",
            [(doc_id, page_index, text) for page_index, text in enumerate(pages)],
        )
        conn.commit()
    except Exception:  # noqa: BLE001
        conn.rollback()
        raise

    return status


def ingest_document(
    conn: sqlite3.Connection,
    pdf_path: Path,
) -> DocumentResult:
    resolved = pdf_path.resolve()
    resolved_path = str(resolved)
    pdf_bytes = resolved.read_bytes()
    sha256 = compute_sha256(pdf_bytes)
    existing = load_existing_document(conn, resolved_path)
    if existing is not None and existing.sha256 == sha256:
        return DocumentResult(status="skipped", page_count=0)

    pages = extract_pages(pdf_bytes, resolved)
    status = write_document(
        conn=conn,
        resolved_path=resolved_path,
        filename=resolved.name,
        sha256=sha256,
        pages=pages,
        existing=existing,
    )
    return DocumentResult(status=status, page_count=len(pages))


def run_ingest(
    pdf_dir: Path, sqlite_path: Path, logger: logging.Logger | None = None
) -> IngestStats:
    if not pdf_dir.exists():
        raise ValueError(f"PDF directory does not exist: {pdf_dir}")
    if not pdf_dir.is_dir():
        raise ValueError(f"PDF path is not a directory: {pdf_dir}")

    active_logger = logger if logger is not None else logging.getLogger(__name__)
    pdfs = discover_pdfs(pdf_dir)
    stats = IngestStats(discovered=len(pdfs))

    conn = connect_db(sqlite_path)
    try:
        ensure_schema(conn)
        for pdf_path in pdfs:
            try:
                result = ingest_document(conn, pdf_path)
            except Exception as exc:  # noqa: BLE001
                stats.failed += 1
                active_logger.error("Failed to ingest %s: %s", pdf_path, exc)
                continue

            if result.status == "ingested":
                stats.ingested += 1
            elif result.status == "updated":
                stats.updated += 1
            else:
                stats.skipped += 1
    finally:
        conn.close()

    return stats
