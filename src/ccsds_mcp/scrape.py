from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import unquote, urljoin, urlparse

import requests

SOURCE_URL = "https://ccsds.org/publications/ccsdsallpubs/"
OUTPUT_DIR = Path("src/ccsds_mcp/resources/pdfs")
METADATA_PATH = OUTPUT_DIR / ".metadata.json"
REQUEST_TIMEOUT = 30
DOWNLOAD_DELAY_SECONDS = 2.0
USER_AGENT = "ccsds-mcp-scraper/0.1 (+respectful-rate-limit)"


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.hrefs.append(value)
                return


class SnippetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value is not None:
                self.hrefs.append(value)
                break

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)


def parse_html_snippet(fragment: str) -> tuple[str, str | None]:
    parser = SnippetParser()
    parser.feed(fragment)
    text = html.unescape(" ".join(" ".join(parser.text_parts).split()))
    first_href = parser.hrefs[0] if parser.hrefs else None
    return text, first_href


def is_allowed_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    hostname = (parsed.hostname or "").lower()
    if hostname != "ccsds.org" and not hostname.endswith(".ccsds.org"):
        return False
    return parsed.path.lower().endswith(".pdf")


def sanitize_filename(name: str) -> str:
    allowed = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    )
    sanitized = "".join(ch if ch in allowed else "_" for ch in name).strip("._")
    if not sanitized:
        sanitized = "document.pdf"
    return sanitized


def filename_for_url(url: str, index: int) -> str:
    parsed = urlparse(url)
    basename = unquote(Path(parsed.path).name)
    if not basename:
        basename = f"document_{index}.pdf"
    if not basename.lower().endswith(".pdf"):
        basename = f"{basename}.pdf"
    return sanitize_filename(basename)


def filename_map_for_urls(urls: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    used_names: dict[str, str] = {}
    for idx, url in enumerate(urls, start=1):
        candidate = filename_for_url(url, idx)
        assigned = candidate
        if assigned in used_names and used_names[assigned] != url:
            suffix = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
            path = Path(candidate)
            assigned = f"{path.stem}_{suffix}{path.suffix}"
        used_names[assigned] = url
        mapping[url] = assigned
    return mapping


def load_metadata() -> dict[str, dict[str, object]]:
    if not METADATA_PATH.exists():
        return {}
    try:
        data = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    normalized: dict[str, dict[str, object]] = {}
    for url, value in data.items():
        if isinstance(url, str) and isinstance(value, dict):
            normalized[url] = value
    return normalized


def save_metadata(metadata: dict[str, dict[str, object]]) -> None:
    payload = json.dumps(metadata, indent=2, sort_keys=True)
    with NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=OUTPUT_DIR,
        prefix=".metadata.",
        suffix=".json.tmp",
        delete=False,
    ) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)
    tmp_path.replace(METADATA_PATH)


def extract_embedded_rows(html: str) -> list[list[str]]:
    key = '"data":['
    start = html.find(key)
    if start < 0:
        return []
    i = start + len(key) - 1

    depth = 0
    in_string = False
    escaping = False
    end = -1
    for pos, ch in enumerate(html[i:], start=i):
        if in_string:
            if escaping:
                escaping = False
            elif ch == "\\":
                escaping = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = pos + 1
                break
    if end < 0:
        return []
    try:
        parsed = json.loads(html[i:end])
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    rows: list[list[str]] = []
    for row in parsed:
        if isinstance(row, list):
            rows.append([str(cell) for cell in row])
    return rows


def extract_publications_from_rows(rows: list[list[str]]) -> dict[str, dict[str, object]]:
    publications: dict[str, dict[str, object]] = {}
    for row in rows:
        if len(row) < 10:
            continue

        file_text, file_href = parse_html_snippet(row[1])
        if not file_href:
            continue
        file_url = urljoin(SOURCE_URL, file_href)
        if not is_allowed_pdf_url(file_url):
            continue

        document_number, entry_href = parse_html_snippet(row[2])
        document_title = html.unescape(" ".join(row[3].split()))
        book_type = html.unescape(" ".join(row[4].split()))
        issue_number = html.unescape(" ".join(row[5].split()))
        published_date = html.unescape(" ".join(row[6].split()))
        description, _ = parse_html_snippet(row[7])
        working_group, working_group_href = parse_html_snippet(row[8])
        iso_equivalent, iso_equivalent_href = parse_html_snippet(row[9])
        iso_equivalent = re.sub(r"^ISO Equivalent\s*:\s*", "", iso_equivalent).strip()

        publications[file_url] = {
            "file": file_text or filename_for_url(file_url, 1),
            "file_url": file_url,
            "document_number": document_number,
            "document_title": document_title,
            "issue_number": issue_number,
            "published_date": published_date,
            "description": description,
            "book_type": book_type,
            "working_group": working_group,
            "working_group_url": urljoin(SOURCE_URL, working_group_href)
            if working_group_href
            else None,
            "iso_equivalent": iso_equivalent,
            "iso_equivalent_url": urljoin(SOURCE_URL, iso_equivalent_href)
            if iso_equivalent_href
            else None,
            "entry_url": urljoin(SOURCE_URL, entry_href) if entry_href else None,
        }
    return publications


def fetch_publications(session: requests.Session) -> dict[str, dict[str, object]]:
    response = session.get(SOURCE_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    html = response.text

    rows = extract_embedded_rows(html)
    publications = extract_publications_from_rows(rows)
    if publications:
        return publications

    parser = LinkParser()
    parser.feed(html)
    resolved_urls = [urljoin(SOURCE_URL, href) for href in parser.hrefs]
    escaped_urls = re.findall(
        r"https:\\/\\/[^\"'\s<>]+?\.pdf",
        html,
        flags=re.IGNORECASE,
    )
    resolved_urls.extend(url.replace("\\/", "/") for url in escaped_urls)
    urls = sorted({url for url in resolved_urls if is_allowed_pdf_url(url)})
    return {
        url: {
            "file": filename_for_url(url, 1),
            "file_url": url,
            "document_number": "",
            "document_title": "",
            "issue_number": "",
            "published_date": "",
            "description": "",
            "book_type": "",
            "working_group": "",
            "working_group_url": None,
            "iso_equivalent": "",
            "iso_equivalent_url": None,
            "entry_url": None,
        }
        for url in urls
    }


def head_metadata(
    session: requests.Session, url: str
) -> dict[str, str | int | None] | None:
    try:
        response = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException:
        return None
    content_length: int | None = None
    raw_length = response.headers.get("Content-Length")
    if raw_length and raw_length.isdigit():
        content_length = int(raw_length)
    return {
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "content_length": content_length,
    }


def should_skip_download(
    destination: Path,
    saved: dict[str, object] | None,
    remote: dict[str, str | int | None] | None,
    expected_filename: str,
) -> bool:
    if not destination.exists() or not saved or not remote:
        return False
    if str(saved.get("filename")) != expected_filename:
        return False
    etag = remote.get("etag")
    last_modified = remote.get("last_modified")
    if not etag or not last_modified:
        return False
    if str(saved.get("etag")) != etag or str(saved.get("last_modified")) != last_modified:
        return False
    remote_length = remote.get("content_length")
    if remote_length is not None and saved.get("content_length") != remote_length:
        return False
    return True


def build_metadata_record(
    *,
    filename: str,
    remote: dict[str, str | int | None] | None,
    publication: dict[str, object] | None,
) -> dict[str, object]:
    remote_data = remote or {}
    return {
        "filename": filename,
        "etag": remote_data.get("etag"),
        "last_modified": remote_data.get("last_modified"),
        "content_length": remote_data.get("content_length"),
        "publication": publication or {},
    }


def download_file(
    session: requests.Session, url: str, destination: Path
) -> tuple[bool, str]:
    tmp_path: Path | None = None
    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            with NamedTemporaryFile(
                mode="wb",
                dir=destination.parent,
                prefix=f".{destination.stem}.",
                suffix=".part",
                delete=False,
            ) as tmp:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        tmp.write(chunk)
                tmp_path = Path(tmp.name)
        tmp_path.replace(destination)
        return True, ""
    except requests.RequestException as err:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False, str(err)
    except OSError as err:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        return False, str(err)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download CCSDS publication PDFs with polite rate limiting."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of PDFs to process after discovery.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        print("ERROR --limit must be a positive integer")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    try:
        publications = fetch_publications(session)
    except requests.RequestException as err:
        print(f"ERROR source page fetch failed: {err}")
        return 1

    urls = sorted(publications.keys())
    if args.limit is not None:
        urls = urls[: args.limit]

    name_map = filename_map_for_urls(urls)
    metadata = load_metadata()

    downloaded_count = 0
    updated_count = 0
    skipped_count = 0
    failed_count = 0

    total = len(urls)
    for idx, url in enumerate(urls, start=1):
        filename = name_map[url]
        destination = OUTPUT_DIR / filename
        saved = metadata.get(url)
        remote = head_metadata(session, url)
        print(f"Processing PDF {idx} of {total}: {filename}")

        if should_skip_download(destination, saved, remote, filename):
            metadata[url] = build_metadata_record(
                filename=filename,
                remote=remote,
                publication=publications.get(url),
            )
            save_metadata(metadata)
            print(f"SKIP [{idx}/{total}] {filename}")
            skipped_count += 1
            continue

        existed_before = destination.exists()
        print(f"Downloading PDF {idx} of {total}: {filename}")
        success, error = download_file(session, url, destination)
        if not success:
            print(f"ERROR [{idx}/{total}] {filename}: {error}")
            failed_count += 1
            continue

        refreshed = remote or head_metadata(session, url) or {}
        metadata[url] = build_metadata_record(
            filename=filename,
            remote=refreshed,
            publication=publications.get(url),
        )
        save_metadata(metadata)

        if existed_before:
            print(f"UPDATED [{idx}/{total}] {filename}")
            updated_count += 1
        else:
            print(f"GET [{idx}/{total}] {filename}")
            downloaded_count += 1

        time.sleep(DOWNLOAD_DELAY_SECONDS)

    save_metadata(metadata)

    print(f"Discovered PDFs: {len(urls)}")
    print(f"Downloaded: {downloaded_count}")
    print(f"Updated: {updated_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Failed: {failed_count}")
    print(f"Metadata: {METADATA_PATH}")

    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
