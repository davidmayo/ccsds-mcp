# ccsds-mcp

This project provides CLI tools for ingesting CCSDS PDFs and searching the
extracted page text with BM25 ranking.

## Requirements

- Python 3.13+
- `uv`

## Install

```bash
uv sync
```

## Usage

```bash
ccsds-mcp ingest ./src/ccsds_mcp/resources/pdfs ./src/ccsds_mcp/resources/database/db.sqlite
```

```bash
ccsds-mcp search ./src/ccsds_mcp/resources/database/db.sqlite "bch generator polynomial" --top-k 5
```

```bash
ccsds-mcp search ./src/ccsds_mcp/resources/database/db.sqlite "the" --top-k 3
```

## Search behavior (v1)

The `search` command builds a BM25 index in memory at runtime from all rows in
the `pages` table. The index is rebuilt on each invocation.

## Verify

Run ingestion:

```bash
ccsds-mcp ingest ./src/ccsds_mcp/resources/pdfs ./src/ccsds_mcp/resources/database/db.sqlite
```

Check page rows:

```bash
sqlite3 ./src/ccsds_mcp/resources/database/db.sqlite "select count(*) from pages;"
```

Run ingestion again and confirm unchanged PDFs are skipped:

```bash
ccsds-mcp ingest ./src/ccsds_mcp/resources/pdfs ./src/ccsds_mcp/resources/database/db.sqlite
```

Run search:

```bash
ccsds-mcp search ./src/ccsds_mcp/resources/database/db.sqlite "bch generator polynomial" --top-k 5
```
