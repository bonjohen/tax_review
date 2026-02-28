"""Download IRS SOI data files and generate SHA-256 manifest.

Usage:
    python -m src.etl.download [--years 2020 2021 2022] [--force] [--verify-only]
"""

import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from .url_registry import get_all_downloads, get_excel_files, get_pdf_file, YEARS

logger = logging.getLogger(__name__)

DATA_ROOT = Path("data")
RAW_DIR = DATA_ROOT / "raw"
PARAMS_DIR = DATA_ROOT / "parameters"
MANIFEST_PATH = DATA_ROOT / "manifest.json"

SESSION_HEADERS = {
    "User-Agent": "tax-review-etl/0.1 (academic research; SOI data download)"
}

MAX_RETRIES = 3
RETRY_DELAYS = [2, 4, 8]


def sha256_file(filepath: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url: str, dest: Path, force: bool = False) -> dict:
    """Download a single file with retry logic.

    Returns dict with download metadata (sha256, size_bytes, download_date).
    """
    if dest.exists() and not force:
        logger.info(f"Skipping {dest.name} (already exists)")
        return {
            "sha256": sha256_file(dest),
            "size_bytes": dest.stat().st_size,
            "download_date": None,
            "skipped": True,
        }

    dest.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    last_error = None
    for attempt, delay in enumerate(RETRY_DELAYS, 1):
        try:
            logger.info(f"Downloading {url} (attempt {attempt}/{MAX_RETRIES})")
            resp = session.get(url, stream=True, timeout=60)
            resp.raise_for_status()

            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            download_date = datetime.now(timezone.utc).isoformat()
            return {
                "sha256": sha256_file(dest),
                "size_bytes": dest.stat().st_size,
                "download_date": download_date,
                "skipped": False,
            }
        except requests.RequestException as e:
            last_error = e
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                import time
                time.sleep(delay)

    raise RuntimeError(f"Failed to download {url} after {MAX_RETRIES} attempts: {last_error}")


def download_year(year: int, force: bool = False) -> list[dict]:
    """Download all files for a given tax year. Returns manifest entries."""
    entries = []

    # Excel files -> data/raw/{year}/
    for table_id, info in get_excel_files(year).items():
        dest = RAW_DIR / str(year) / info["filename"]
        meta = download_file(info["url"], dest, force=force)
        entries.append({
            "url": info["url"],
            "filename": info["filename"],
            "year": year,
            "table_id": table_id,
            "local_path": str(dest),
            "file_type": "excel",
            **meta,
        })

    # Revenue Procedure PDF -> data/parameters/
    pdf_info = get_pdf_file(year)
    dest = PARAMS_DIR / pdf_info["filename"]
    meta = download_file(pdf_info["url"], dest, force=force)
    entries.append({
        "url": pdf_info["url"],
        "filename": pdf_info["filename"],
        "year": year,
        "table_id": "rev_proc",
        "local_path": str(dest),
        "file_type": "pdf",
        **meta,
    })

    return entries


def write_manifest(entries: list[dict]) -> None:
    """Write the download manifest to data/manifest.json."""
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": entries,
    }
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info(f"Manifest written to {MANIFEST_PATH} ({len(entries)} files)")


def verify_manifest() -> bool:
    """Verify all files in manifest match their recorded checksums."""
    if not MANIFEST_PATH.exists():
        logger.error("No manifest found")
        return False

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    all_ok = True
    for entry in manifest["files"]:
        path = Path(entry["local_path"])
        if not path.exists():
            logger.error(f"MISSING: {path}")
            all_ok = False
            continue
        actual = sha256_file(path)
        if actual != entry["sha256"]:
            logger.error(f"MISMATCH: {path} expected {entry['sha256']}, got {actual}")
            all_ok = False
        else:
            logger.info(f"OK: {path}")

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Download IRS SOI data files")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    parser.add_argument("--force", action="store_true", help="Re-download even if file exists")
    parser.add_argument("--verify-only", action="store_true", help="Only verify existing manifest")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.verify_only:
        ok = verify_manifest()
        raise SystemExit(0 if ok else 1)

    all_entries = []
    for year in args.years:
        entries = download_year(year, force=args.force)
        all_entries.extend(entries)

    write_manifest(all_entries)
    logger.info("Download complete.")


if __name__ == "__main__":
    main()
