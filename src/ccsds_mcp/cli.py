from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

from ccsds_mcp.ingest import IngestStats, run_ingest
from ccsds_mcp.search import format_hits, search_pages

LOGGER = logging.getLogger("ccsds_mcp")


def configure_logging() -> None:
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    LOGGER.handlers.clear()
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ccsds-mcp",
        description="Ingestion and search tools for CCSDS PDF text extraction.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest PDF pages into SQLite.",
    )
    ingest_parser.add_argument("pdf_dir", help="Directory to scan recursively for PDFs.")
    ingest_parser.add_argument("sqlite_path", help="Destination SQLite database path.")

    search_parser = subparsers.add_parser(
        "search",
        help="Search ingested pages with BM25 ranking.",
    )
    search_parser.add_argument("sqlite_path", help="Source SQLite database path.")
    search_parser.add_argument("query", help="Search query.")
    search_parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Maximum number of ranked hits to print (default: 5).",
    )

    return parser


def print_summary(stats: IngestStats) -> None:
    print(f"Discovered PDFs: {stats.discovered}")
    print(f"Ingested new: {stats.ingested}")
    print(f"Updated changed: {stats.updated}")
    print(f"Skipped unchanged: {stats.skipped}")
    print(f"Failed: {stats.failed}")


def handle_ingest(args: argparse.Namespace) -> int:
    stats = run_ingest(
        pdf_dir=Path(args.pdf_dir),
        sqlite_path=Path(args.sqlite_path),
        logger=LOGGER,
    )
    print_summary(stats)
    return 1 if stats.failed > 0 else 0


def handle_search(args: argparse.Namespace) -> int:
    hits = search_pages(
        sqlite_path=Path(args.sqlite_path),
        query=args.query,
        top_k=args.top_k,
    )
    for line in format_hits(hits):
        print(line)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "ingest":
            return handle_ingest(args)
        if args.command == "search":
            return handle_search(args)
    except ValueError as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Unexpected error: %s", exc)
        return 1

    LOGGER.error("Unknown command: %s", args.command)
    return 1
