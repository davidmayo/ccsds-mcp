# ccsds-mcp

This project currently implements only an ingestion CLI for CCSDS PDFs.

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
