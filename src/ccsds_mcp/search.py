from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from rank_bm25 import BM25Okapi

from ccsds_mcp.db import connect_db

TOKEN_SPLIT_RE = re.compile(r"[^0-9A-Za-z]+")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(slots=True)
class SearchDocument:
    filename: str
    path: str
    page_index: int
    text: str
    tokens: list[str]


@dataclass(slots=True)
class SearchHit:
    rank_index: int
    filename: str
    path: str
    page_index: int
    score: float
    snippet: str


def tokenize(text: str) -> list[str]:
    return [token for token in TOKEN_SPLIT_RE.split(text.lower()) if token]


def make_snippet(text: str, max_chars: int = 240) -> str:
    if max_chars < 1:
        raise ValueError("max_chars must be at least 1")

    single_line = WHITESPACE_RE.sub(" ", text).strip()
    if len(single_line) <= max_chars:
        return single_line
    if max_chars <= 3:
        return "." * max_chars
    return f"{single_line[: max_chars - 3].rstrip()}..."


def load_corpus(conn: sqlite3.Connection) -> list[SearchDocument]:
    cursor = conn.execute(
        """
        SELECT p.doc_id, p.page_index, p.text, d.filename, d.path
        FROM pages p
        JOIN documents d ON d.doc_id = p.doc_id
        ORDER BY d.filename ASC, p.page_index ASC, d.path ASC
        """
    )

    corpus: list[SearchDocument] = []
    for row in cursor:
        text = str(row["text"])
        corpus.append(
            SearchDocument(
                filename=str(row["filename"]),
                path=str(row["path"]),
                page_index=int(row["page_index"]),
                text=text,
                tokens=tokenize(text),
            )
        )
    return corpus


def search_pages(sqlite_path: Path, query: str, top_k: int) -> list[SearchHit]:
    if top_k <= 0:
        raise ValueError("--top-k must be greater than 0")
    if not sqlite_path.exists():
        raise ValueError(f"SQLite database does not exist: {sqlite_path}")
    if not sqlite_path.is_file():
        raise ValueError(f"SQLite path is not a file: {sqlite_path}")

    conn = connect_db(sqlite_path)
    try:
        corpus = load_corpus(conn)
    finally:
        conn.close()

    if not corpus:
        return []

    query_tokens = tokenize(query)
    if not query_tokens:
        return []

    bm25 = BM25Okapi([document.tokens for document in corpus])
    scores = bm25.get_scores(query_tokens)

    scored_indices: list[tuple[int, float]] = [
        (index, float(score))
        for index, score in enumerate(scores)
        if float(score) > 0.0
    ]
    scored_indices.sort(
        key=lambda item: (
            -item[1],
            corpus[item[0]].filename,
            corpus[item[0]].page_index,
            corpus[item[0]].path,
            item[0],
        )
    )

    hits: list[SearchHit] = []
    for rank_index, (index, score) in enumerate(scored_indices[:top_k], start=1):
        document = corpus[index]
        hits.append(
            SearchHit(
                rank_index=rank_index,
                filename=document.filename,
                path=document.path,
                page_index=document.page_index,
                score=score,
                snippet=make_snippet(document.text),
            )
        )
    return hits


def format_hits(hits: list[SearchHit]) -> list[str]:
    if not hits:
        return ["No results."]

    output_lines: list[str] = []
    for hit in hits:
        output_lines.append(
            f"{hit.rank_index}. {hit.filename}:p{hit.page_index + 1} score={hit.score:.4f}"
        )
        output_lines.append(f"  {hit.snippet}")
    return output_lines
