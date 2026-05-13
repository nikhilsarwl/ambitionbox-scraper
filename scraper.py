"""
AmbitionBox Company Scraper
============================
Scrapes 5 listing pages from https://www.ambitionbox.com/list-of-companies,
visits each company profile, and saves structured data to companies.csv.

Usage:
    pip install requests beautifulsoup4 lxml
    python scraper.py

Output: companies.csv  (50 rows, 12 columns)
"""

from __future__ import annotations

import csv
import re
import time
from dataclasses import dataclass, fields
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Config

BASE_URL      = "https://www.ambitionbox.com"
LISTING_URL   = f"{BASE_URL}/list-of-companies"
NUM_PAGES     = 5
MAX_COMPANIES = 50
DELAY_SEC     = 1.5          # polite delay between requests
OUTPUT_CSV    = "companies.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Category rating labels as they appear on the page
RATING_LABELS: dict[str, list[str]] = {
    "rating_salary":       ["Salary & Benefits", "Salary"],
    "rating_job_security": ["Job Security"],
    "rating_work_life":    ["Work-Life Balance", "Work Life Balance"],
    "rating_skill_dev":    ["Skill Development", "Skill Development / Learning"],
    "rating_culture":      ["Company Culture", "Culture"],
    "rating_career":       ["Career Growth", "Promotions / Appraisal", "Promotions"],
}

# Units for review counts like "1.1L" or "73.5k"
REVIEW_UNITS = {"k": 1_000, "K": 1_000, "l": 100_000, "L": 100_000, "m": 1_000_000, "M": 1_000_000}


# Data model

@dataclass
class Company:
    company_name:       str             = "N/A"
    profile_url:        str             = "N/A"
    overall_rating:     Optional[float] = None
    total_reviews:      Optional[int]   = None
    industries:         str             = "N/A"
    description:        str             = "N/A"
    rating_salary:      Optional[float] = None
    rating_job_security: Optional[float] = None
    rating_work_life:   Optional[float] = None
    rating_skill_dev:   Optional[float] = None
    rating_culture:     Optional[float] = None
    rating_career:      Optional[float] = None

    def to_row(self) -> dict:
        row = {}
        for f in fields(self):
            v = getattr(self, f.name)
            row[f.name] = v if v is not None else "N/A"
        return row

    @property
    def csv_columns(self) -> list[str]:
        return [f.name for f in fields(self)]


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def fetch(url: str, retries: int = 3) -> Optional[str]:
    """GET with retries and exponential backoff. Returns HTML or None."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    [warn] fetch attempt {attempt+1} failed ({e}); retrying in {wait}s")
            time.sleep(wait)
    print(f"    [error] Giving up on {url}")
    return None


# ---------------------------------------------------------------------------
# Step 1 — collect company links from listing pages
# ---------------------------------------------------------------------------

def get_listing_links(num_pages: int = NUM_PAGES) -> list[tuple[str, str]]:
    """Return [(name, profile_url), ...] from the first `num_pages` listing pages."""
    seen:    set[str]            = set()
    results: list[tuple[str,str]] = []

    for page in range(1, num_pages + 1):
        url  = LISTING_URL if page == 1 else f"{LISTING_URL}?page={page}"
        print(f"[listing] Fetching page {page}: {url}")
        html = fetch(url)
        if not html:
            continue

        soup = BeautifulSoup(html, "lxml")

        # Strategy A: any anchor linking to an /overview/ page
        anchors = soup.select("a[href*='/overview/']")
        for a in anchors:
            href = a.get("href", "").split("?")[0].strip()
            if not href:
                continue
            full = urljoin(BASE_URL, href)
            if full in seen:
                continue
            name = _clean_text(a) or _name_from_url(full)
            seen.add(full)
            results.append((name, full))

        # Strategy B: fallback on card wrappers
        if not anchors:
            for card in soup.select("[class*='companyCardWrapper'], [class*='company-card']"):
                a = card.find("a", href=True)
                if not a:
                    continue
                href = a["href"].split("?")[0].strip()
                full = urljoin(BASE_URL, href)
                if full in seen:
                    continue
                name = _clean_text(card) or _name_from_url(full)
                seen.add(full)
                results.append((name, full))

        print(f"           → {len(results)} unique companies so far")
        time.sleep(DELAY_SEC)

    return results


# ---------------------------------------------------------------------------
# Step 2 — extract company details from profile page
# ---------------------------------------------------------------------------

def extract_company(name: str, url: str, html: str) -> Company:
    soup = BeautifulSoup(html, "lxml")
    c    = Company(company_name=name, profile_url=url)
    text = soup.get_text("\n", strip=True)

    # Company name from h1 (more reliable than listing card text)
    h1 = soup.select_one("h1")
    if h1:
        h1_text = _clean_text(h1)
        if h1_text and len(h1_text) < 80:
            c.company_name = h1_text

    # Overall rating + review count
    c.overall_rating, c.total_reviews = _extract_rating_and_reviews(soup, text)

    # Industries
    c.industries = _extract_industries(soup, text) or "N/A"

    # Description / About
    c.description = _extract_description(soup, text) or "N/A"

    # Category ratings
    c.rating_salary       = _find_category_rating(soup, text, RATING_LABELS["rating_salary"])
    c.rating_job_security = _find_category_rating(soup, text, RATING_LABELS["rating_job_security"])
    c.rating_work_life    = _find_category_rating(soup, text, RATING_LABELS["rating_work_life"])
    c.rating_skill_dev    = _find_category_rating(soup, text, RATING_LABELS["rating_skill_dev"])
    c.rating_culture      = _find_category_rating(soup, text, RATING_LABELS["rating_culture"])
    c.rating_career       = _find_category_rating(soup, text, RATING_LABELS["rating_career"])

    return c


# ---------------------------------------------------------------------------
# Field extractors
# ---------------------------------------------------------------------------

def _extract_rating_and_reviews(soup: BeautifulSoup, text: str) -> tuple[Optional[float], Optional[int]]:
    # Pattern: "3.3\nbased on 1.1L Reviews"
    m = re.search(r"(\d\.\d)\s*[\n ]+based on\s+([\d.]+[kKlLmM]?)\s*[Rr]eviews?", text)
    if m:
        return _to_float(m.group(1)), _parse_review_count(m.group(2))

    # Fallback: find a rating element
    for el in soup.select("[class*='rating'], [class*='Rating']"):
        m2 = re.search(r"\b(\d\.\d)\b", el.get_text())
        if m2:
            rating = _to_float(m2.group(1))
            # Reviews nearby in full text
            rev_m = re.search(r"([\d.,]+[kKlLmM]?)\s*[Rr]eviews?", text)
            reviews = _parse_review_count(rev_m.group(1)) if rev_m else None
            return rating, reviews

    return None, None


def _extract_industries(soup: BeautifulSoup, text: str) -> Optional[str]:
    # Non-industry labels that use the same URL pattern (company types, not industries)
    EXCLUDE = {"public", "private", "startup", "mnc", "indian", "foreign"}

    # 1. Links matching "/<slug>-companies-in-india" — reliable on both reviews & overview pages
    industries: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/[\w-]+-companies-in-india$", href):
            label = _clean_text(a)
            if label and label.lower() not in EXCLUDE and label not in industries:
                industries.append(label)
    if industries:
        return " | ".join(industries)

    # 2. Text pattern on overview pages: "Primary Industry\n...\n<value>"
    m = re.search(
        r"Primary Industry\s*\n"
        r"[^\n]*\n"            # description line
        r"([^\n]{3,60})",
        text,
    )
    if m:
        return m.group(1).strip()

    # 3. Listing-card pattern: "Industry | Location" (e.g. "IT Services & Consulting | Bengaluru")
    m = re.search(r"\n([A-Z][\w &/-]{2,50})\s*\|.*?location", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()

    return None


def _extract_description(soup: BeautifulSoup, text: str) -> Optional[str]:
    # 1. Dedicated About/Description elements
    for selector in [
        "[class*='aboutCompany']", "[class*='about-company']",
        "[class*='About']", "[class*='companyOverview']",
        "[class*='description']",
    ]:
        el = soup.select_one(selector)
        if el:
            t = el.get_text(" ", strip=True)
            t = re.sub(r"\s+", " ", t).strip()
            if 60 < len(t) < 1500:
                return t

    # 2. Regex patterns in text
    for pattern in [
        r"About\s+\S[\w &.,'-]+\s*\n([^\n]{80,800})",
        r"About the [Cc]ompany\s*\n([^\n]{60,800})",
        r"Overview\s*\n([^\n]{80,800})",
    ]:
        m = re.search(pattern, text)
        if m:
            d = m.group(1).strip()
            if 60 < len(d) < 1500 and "Reviews" not in d[:30]:
                return d

    # 3. Longest plausible paragraph in first 4000 chars
    head = text[:4000]
    candidates = [
        p.strip() for p in head.split("\n")
        if 80 < len(p.strip()) < 800
        and "Reviews" not in p and "Salary" not in p
        and "Rating" not in p
    ]
    if candidates:
        return re.sub(r"\s+", " ", max(candidates, key=len))

    return None


def _find_category_rating(soup: BeautifulSoup, text: str, labels: list[str]) -> Optional[float]:
    """Rating appears on the line BEFORE or AFTER the label in AmbitionBox."""
    for label in labels:
        # Rating before label
        m = re.search(rf"(\d\.\d)\s*\n\s*{re.escape(label)}\b", text)
        if m:
            return _to_float(m.group(1))
        # Rating after label
        m = re.search(rf"{re.escape(label)}\b\s*\n\s*(\d\.\d)", text)
        if m:
            return _to_float(m.group(1))
        # Inline: "Salary 3.8" or "3.8 Salary"
        m = re.search(rf"{re.escape(label)}\b[^\n]{{0,20}}(\d\.\d)", text, re.IGNORECASE)
        if m:
            return _to_float(m.group(1))

    return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _clean_text(node) -> str:
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

def _name_from_url(url: str) -> str:
    """Derive a name from a slug like /reviews/tata-consultancy-services-reviews."""
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"-?(reviews?|overview)$", "", slug)
    return " ".join(w.upper() if len(w) <= 3 else w.capitalize() for w in slug.split("-"))

def _to_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except (TypeError, ValueError):
        return None

def _parse_review_count(s: str) -> Optional[int]:
    """Convert '1.1L' → 110000, '73.5k' → 73500, '1234' → 1234."""
    m = re.match(r"^([\d.,]+)([kKlLmM])?$", s.strip().replace(",", ""))
    if not m:
        return None
    try:
        n = float(m.group(1))
    except ValueError:
        return None
    if m.group(2):
        n *= REVIEW_UNITS[m.group(2)]
    return int(round(n))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("AmbitionBox Scraper")
    print("=" * 60)

    # Step 1: collect links
    links = get_listing_links(NUM_PAGES)
    links = links[:MAX_COMPANIES]
    print(f"\n[info] Processing {len(links)} companies\n")

    companies: list[Company] = []

    for i, (name, url) in enumerate(links, start=1):
        print(f"[{i:02d}/{len(links)}] {name}")
        print(f"         {url}")

        html = fetch(url)
        if not html:
            companies.append(Company(company_name=name, profile_url=url))
            print("         → FAILED (stub row)")
            time.sleep(DELAY_SEC)
            continue

        company = extract_company(name, url, html)
        companies.append(company)

        filled = sum(
            1 for f in fields(company)
            if f.name not in ("company_name", "profile_url")
            and getattr(company, f.name) is not None
            and getattr(company, f.name) != "N/A"
        )
        print(f"         → OK  ({filled}/10 fields filled)")
        time.sleep(DELAY_SEC)

    # Step 2: write CSV
    if not companies:
        print("\n[error] No companies scraped. Check your connection.")
        return

    col_names = [f.name for f in fields(Company)]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=col_names)
        writer.writeheader()
        for c in companies:
            writer.writerow(c.to_row())

    # Step 3: summary
    print(f"\n{'='*60}")
    print(f"[done] Wrote {len(companies)} rows → {OUTPUT_CSV}")
    print("\nFill rates:")
    for col in col_names[2:]:   # skip name & url
        filled = sum(1 for c in companies if getattr(c, col) not in (None, "N/A"))
        pct = round(filled / len(companies) * 100)
        print(f"  {col:>22}: {filled:>2}/{len(companies)}  ({pct}%)")


if __name__ == "__main__":
    main()
