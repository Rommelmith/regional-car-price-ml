"""Polite, resumable command-line scraper for public PakWheels search listings."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .constants import DEFAULT_DATA_PATH

LOGGER = logging.getLogger(__name__)
SEARCH_URL = "https://www.pakwheels.com/used-cars/search/-/"
CSV_FIELDS = [
    "title",
    "price",
    "year",
    "mileage",
    "fuel_type",
    "engine_capacity",
    "transmission",
    "link",
]
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
)


def parse_engine_specs(engine_text: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in re.split(r"\s+[.·]\s+", engine_text) if part.strip()]
    padded = (parts + ["", "", ""])[:3]
    return padded[0], padded[1], padded[2]


def _json_ld_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        if "@graph" in value and isinstance(value["@graph"], list):
            return next((item for item in value["@graph"] if isinstance(item, dict)), None)
        return value
    if isinstance(value, list):
        return next((item for item in value if isinstance(item, dict)), None)
    return None


def parse_search_page(html: str) -> list[dict[str, str]]:
    """Parse one search page without performing network I/O."""

    soup = BeautifulSoup(html, "lxml")
    cars: list[dict[str, str]] = []
    for listing in soup.select("ul.search-results li.classified-listing"):
        script = listing.find("script", attrs={"type": "application/ld+json"})
        if script is None or not script.string:
            continue
        try:
            data = _json_ld_object(json.loads(script.string))
        except (TypeError, json.JSONDecodeError):
            continue
        if not data:
            continue

        offers = data.get("offers") or {}
        if isinstance(offers, list):
            offers = next((offer for offer in offers if isinstance(offer, dict)), {})
        if not isinstance(offers, dict):
            offers = {}

        title = str(data.get("name") or "").strip()
        link = str(offers.get("url") or "").strip()
        if not title or not link:
            continue
        year = str(data.get("modelDate") or "").strip()
        price = offers.get("price")
        currency = str(offers.get("priceCurrency") or "PKR").strip()
        if isinstance(price, (int, float)):
            price_text = f"{currency} {price:,.0f}"
        else:
            price_text = str(price or "").strip()

        mileage = fuel_type = engine_capacity = transmission = ""
        specifications = listing.select_one("ul.ad-specs")
        if specifications:
            for item in specifications.find_all("li"):
                icon = item.find("i")
                classes = icon.get("class", []) if icon else []
                text = item.get_text(" ", strip=True)
                if any("pw-mileage" in name for name in classes):
                    mileage = text
                elif any("pw-engine" in name for name in classes):
                    fuel_type, engine_capacity, transmission = parse_engine_specs(text)

        cars.append(
            {
                "title": title,
                "price": price_text,
                "year": year,
                "mileage": mileage,
                "fuel_type": fuel_type,
                "engine_capacity": engine_capacity,
                "transmission": transmission,
                "link": link,
            }
        )
    return cars


def build_session(*, retries: int, user_agent: str) -> requests.Session:
    retry_policy = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent, "Accept-Language": "en-US,en;q=0.9"})
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def write_csv_atomic(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".partial.csv", dir=output.parent, text=True
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        temporary_path.replace(output)
        output.chmod(0o644)
    finally:
        temporary_path.unlink(missing_ok=True)


def load_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def scrape(
    *,
    output: Path,
    start_page: int,
    pages: int,
    delay: float,
    timeout: float,
    retries: int,
    checkpoint_every: int,
    stop_after_empty: int,
    resume: bool,
    user_agent: str,
) -> dict[str, int]:
    rows = load_existing_rows(output) if resume else []
    known_links = {row["link"] for row in rows if row.get("link")}
    duplicates_skipped = 0
    empty_pages = 0
    pages_requested = 0

    with build_session(retries=retries, user_agent=user_agent) as session:
        try:
            for page in range(start_page, start_page + pages):
                pages_requested += 1
                LOGGER.info("Scraping page %s", page)
                response = session.get(SEARCH_URL, params={"page": page}, timeout=timeout)
                response.raise_for_status()
                page_rows = parse_search_page(response.text)
                if not page_rows:
                    empty_pages += 1
                    LOGGER.warning(
                        "No listings found on page %s (%s consecutive)", page, empty_pages
                    )
                    if empty_pages >= stop_after_empty:
                        LOGGER.info("Stopping after %s consecutive empty pages", empty_pages)
                        break
                else:
                    empty_pages = 0
                    for row in page_rows:
                        if row["link"] in known_links:
                            duplicates_skipped += 1
                            continue
                        rows.append(row)
                        known_links.add(row["link"])
                    LOGGER.info("Collected %s unique listings", len(rows))

                if checkpoint_every and pages_requested % checkpoint_every == 0:
                    write_csv_atomic(rows, output)
                    LOGGER.info("Checkpoint written to %s", output)
                if delay:
                    time.sleep(delay)
        finally:
            if rows:
                write_csv_atomic(rows, output)

    return {
        "pages_requested": pages_requested,
        "unique_listings": len(rows),
        "duplicates_skipped": duplicates_skipped,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_DATA_PATH)
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--pages", type=int, default=100)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--checkpoint-every", type=int, default=20)
    parser.add_argument("--stop-after-empty", type=int, default=3)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    arguments = build_parser().parse_args(argv)
    if arguments.start_page < 1 or arguments.pages < 1:
        raise SystemExit("--start-page and --pages must be positive integers")
    logging.basicConfig(
        level=logging.DEBUG if arguments.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )
    summary = scrape(
        output=arguments.output,
        start_page=arguments.start_page,
        pages=arguments.pages,
        delay=max(arguments.delay, 0),
        timeout=arguments.timeout,
        retries=max(arguments.retries, 0),
        checkpoint_every=max(arguments.checkpoint_every, 0),
        stop_after_empty=max(arguments.stop_after_empty, 1),
        resume=arguments.resume,
        user_agent=arguments.user_agent,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
