"""
job_pipeline.py
================
Collects Data Engineer & ML Engineer postings across SE, NO, DE, NL.
Transforms them into a common schema and stores in a single SQLite file.

Sources (all free):
  - JobTech Dev  (SE)        no API key required
  - NAV          (NO)        no API key required
  - Adzuna       (DE, NL)    free tier, requires app_id + app_key
  - Arbeitnow    (DE/NL/remote) no API key required

Setup:
  pip install requests
  # Optional (recommended) - free signup at https://developer.adzuna.com/
  export ADZUNA_APP_ID="your_id"
  export ADZUNA_APP_KEY="your_key"

Usage:
  python job_pipeline.py             # run full collection
  python job_pipeline.py --stats     # print summary
  python job_pipeline.py --export    # export jobs.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Optional

import requests

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
DB_PATH = Path(os.environ.get("JOB_DB_PATH", "jobs.db"))
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "8c0e11bf")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "b8a13080ebfe75a85cd8455edd7b4d4f")

# (query string, normalized role bucket)
QUERIES: list[tuple[str, str]] = [
    ("data engineer", "data_engineer"),
    ("machine learning engineer", "ml_engineer"),
    ("ml engineer", "ml_engineer"),
]

ADZUNA_COUNTRIES = ["de", "nl"]          # Norway is NOT covered by Adzuna
MAX_PAGES_PER_QUERY = 5                  # be polite to free tiers
HTTP_TIMEOUT = 30
USER_AGENT = "job-pipeline/1.0 (personal research)"

# Arbeitnow blocks default UAs; use a real browser string.
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

# NAV's new feed (post-May 2025) is a continuous JSON feed.
NAV_BASE = "https://pam-stilling-feed.nav.no"
NAV_PUBLIC_TOKEN_URL = f"{NAV_BASE}/api/publicToken"
NAV_FEED_START = "/api/v1/feed"
NAV_LOOKBACK_DAYS = 30                   # how far back to walk the feed
MAX_NAV_PAGES = 30                       # safety cap
_nav_token_cache: Optional[str] = None


# ----------------------------------------------------------------------------
# Common data model
# ----------------------------------------------------------------------------
@dataclass
class Job:
    source: str
    external_id: str
    title: str
    company: Optional[str]
    location: Optional[str]
    country: str                # ISO-2 (SE/NO/DE/NL) or "REMOTE"
    description: Optional[str]
    url: str
    posted_at: Optional[str]    # ISO-8601
    seniority: str              # junior | mid | senior | unspecified
    role_category: str          # data_engineer | ml_engineer
    salary_min: Optional[float]
    salary_max: Optional[float]
    currency: Optional[str]
    remote: Optional[bool]
    raw_json: str               # keep the source payload for future re-parsing
    fetched_at: str


# ----------------------------------------------------------------------------
# Seniority detection (rough, but useful for filtering)
# ----------------------------------------------------------------------------
JUNIOR_RE = re.compile(
    r"\b(junior|jr\.?|entry[- ]level|graduate|trainee|associate|"
    r"nyutexaminerad|nyutexad|berufseinsteiger|absolvent|starter)\b",
    re.I,
)
SENIOR_RE = re.compile(
    r"\b(senior|sr\.?|lead|principal|staff|head of|architect|expert)\b", re.I
)
MID_RE = re.compile(r"\b(mid[- ]level|mid[- ]senior|intermediate|medior)\b", re.I)


def detect_seniority(title: str, description: str = "") -> str:
    text = f"{title or ''} {description or ''}"
    if JUNIOR_RE.search(text):
        return "junior"
    if MID_RE.search(text):
        return "mid"
    if SENIOR_RE.search(text):
        return "senior"
    return "unspecified"


# ----------------------------------------------------------------------------
# Database
# ----------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  source        TEXT NOT NULL,
  external_id   TEXT NOT NULL,
  title         TEXT NOT NULL,
  company       TEXT,
  location      TEXT,
  country       TEXT,
  description   TEXT,
  url           TEXT,
  posted_at     TEXT,
  seniority     TEXT,
  role_category TEXT,
  salary_min    REAL,
  salary_max    REAL,
  currency      TEXT,
  remote        INTEGER,
  raw_json      TEXT,
  fetched_at    TEXT,
  UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_jobs_country   ON jobs(country);
CREATE INDEX IF NOT EXISTS idx_jobs_role      ON jobs(role_category);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs(seniority);
CREATE INDEX IF NOT EXISTS idx_jobs_posted    ON jobs(posted_at);

CREATE TABLE IF NOT EXISTS fetch_runs (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  source          TEXT,
  country         TEXT,
  query           TEXT,
  results_count   INTEGER,
  inserted_count  INTEGER,
  started_at      TEXT,
  finished_at     TEXT,
  error           TEXT
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def upsert_job(conn: sqlite3.Connection, job: Job) -> bool:
    """Insert if new. Returns True if inserted, False if duplicate."""
    cur = conn.execute(
        """
        INSERT INTO jobs (
            source, external_id, title, company, location, country,
            description, url, posted_at, seniority, role_category,
            salary_min, salary_max, currency, remote, raw_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(source, external_id) DO NOTHING
        """,
        (
            job.source, job.external_id, job.title, job.company, job.location,
            job.country, job.description, job.url, job.posted_at, job.seniority,
            job.role_category, job.salary_min, job.salary_max, job.currency,
            int(job.remote) if job.remote is not None else None,
            job.raw_json, job.fetched_at,
        ),
    )
    return cur.rowcount > 0


# ----------------------------------------------------------------------------
# HTTP helper
# ----------------------------------------------------------------------------
def http_get(url: str, params: dict | None = None) -> Optional[dict]:
    try:
        r = requests.get(
            url, params=params,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logging.warning(f"HTTP error for {url}: {e}")
        return None


# ----------------------------------------------------------------------------
# Collectors — one per source
# ----------------------------------------------------------------------------

# ----- 1. JobTech Dev (Sweden) ----------------------------------------------
def fetch_jobtech(query: str, role_category: str) -> Iterator[Job]:
    url = "https://jobsearch.api.jobtechdev.se/search"
    offset, limit = 0, 100
    while offset < 1000:  # API hard cap
        data = http_get(url, {"q": query, "offset": offset, "limit": limit})
        if not data:
            return
        hits = data.get("hits", [])
        if not hits:
            return
        for hit in hits:
            yield _parse_jobtech(hit, role_category)
        total = (data.get("total") or {}).get("value", 0)
        offset += limit
        if offset >= total:
            return
        time.sleep(0.3)


def _parse_jobtech(hit: dict, role_category: str) -> Job:
    desc = (hit.get("description") or {}).get("text") or ""
    title = hit.get("headline") or ""
    salary = hit.get("salary_description") or ""
    return Job(
        source="jobtech",
        external_id=str(hit.get("id")),
        title=title,
        company=(hit.get("employer") or {}).get("name"),
        location=(hit.get("workplace_address") or {}).get("municipality"),
        country="SE",
        description=desc,
        url=hit.get("webpage_url") or "",
        posted_at=hit.get("publication_date"),
        seniority=detect_seniority(title, desc),
        role_category=role_category,
        salary_min=None, salary_max=None,
        currency="SEK" if salary else None,
        remote=bool(re.search(r"\b(remote|distans)\b", f"{title} {desc[:500]}", re.I)),
        raw_json=json.dumps(hit, ensure_ascii=False),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


# ----- 2. NAV / pam-stilling-feed (Norway) ----------------------------------
def _get_nav_token() -> Optional[str]:
    """Fetch and cache NAV's rotating public bearer token.

    The /api/publicToken endpoint returns plaintext like:
        Current public token for Nav Job Vacancy Feed:
        eyJhbGciOi...
    so we extract just the JWT (the eyJ... part).
    """
    global _nav_token_cache
    if _nav_token_cache:
        return _nav_token_cache
    try:
        r = requests.get(
            NAV_PUBLIC_TOKEN_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT,
        )
        r.raise_for_status()
        match = re.search(r"ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", r.text)
        if not match:
            logging.warning("NAV token endpoint returned no recognizable JWT.")
            return None
        _nav_token_cache = match.group(0)
        return _nav_token_cache
    except requests.RequestException as e:
        logging.warning(f"Could not fetch NAV public token: {e}")
        return None


def fetch_nav_all(queries: list[tuple[str, str]]) -> Iterator[Job]:
    """
    NAV's new feed is a continuous chronological stream of ALL ads,
    not keyword-searchable. We walk recent pages once, filter client-side
    against ALL queries, and yield matches once with the matching role.
    """
    token = _get_nav_token()
    if not token:
        logging.info("Skipping NAV — no public token available.")
        return

    since = (datetime.now(timezone.utc) - timedelta(days=NAV_LOOKBACK_DAYS))
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "If-Modified-Since": since.strftime("%a, %d %b %Y %H:%M:%S GMT"),
    }
    queries_lower = [(q.lower(), role) for q, role in queries]

    feed_url = NAV_BASE + NAV_FEED_START
    pages = 0
    while feed_url and pages < MAX_NAV_PAGES:
        try:
            r = requests.get(feed_url, headers=headers, timeout=HTTP_TIMEOUT)
            if r.status_code == 304:        # nothing new since last fetch
                return
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logging.warning(f"NAV fetch failed at page {pages}: {e}")
            return

        items = data.get("items", [])
        if not items:
            break
        for item in items:
            entry = item.get("_feed_entry") or {}
            if entry.get("status") != "ACTIVE":
                continue
            title = item.get("title") or entry.get("title") or ""
            content = item.get("content_text") or ""
            haystack = f"{title} {content[:1500]}".lower()
            for q_low, role in queries_lower:
                if q_low in haystack:
                    yield _parse_nav(item, role)
                    break

        next_url = data.get("next_url")
        if not next_url or data.get("next_id") is None:
            return
        feed_url = NAV_BASE + next_url
        pages += 1
        time.sleep(0.3)


def _parse_nav(item: dict, role_category: str) -> Job:
    entry = item.get("_feed_entry") or {}
    title = item.get("title") or entry.get("title") or ""
    content = item.get("content_text") or ""
    url_path = item.get("url") or ""
    return Job(
        source="nav",
        external_id=str(entry.get("uuid") or item.get("id") or ""),
        title=title,
        company=entry.get("businessName"),
        location=entry.get("municipal"),
        country="NO",
        description=content,
        url=NAV_BASE + url_path if url_path.startswith("/") else (url_path or ""),
        posted_at=item.get("date_modified") or entry.get("sistEndret"),
        seniority=detect_seniority(title, content),
        role_category=role_category,
        salary_min=None, salary_max=None, currency=None,
        remote=bool(re.search(r"\b(remote|hjemmekontor|fjernarbeid)\b",
                              f"{title} {content[:500]}", re.I)),
        raw_json=json.dumps(item, ensure_ascii=False),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


# ----- 3. Adzuna (Germany, Netherlands) -------------------------------------
def fetch_adzuna(query: str, role_category: str, country: str) -> Iterator[Job]:
    if not (ADZUNA_APP_ID and ADZUNA_APP_KEY):
        logging.info(f"Skipping Adzuna ({country}) — no credentials set.")
        return
    base = f"https://api.adzuna.com/v1/api/jobs/{country}/search"
    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        data = http_get(f"{base}/{page}", {
            "app_id": ADZUNA_APP_ID,
            "app_key": ADZUNA_APP_KEY,
            "results_per_page": 50,
            "what": query,
            "content-type": "application/json",
        })
        if not data:
            return
        results = data.get("results", [])
        if not results:
            return
        for item in results:
            yield _parse_adzuna(item, role_category, country)
        if len(results) < 50:
            return
        time.sleep(0.5)


def _parse_adzuna(item: dict, role_category: str, country: str) -> Job:
    title = item.get("title") or ""
    desc = item.get("description") or ""
    return Job(
        source="adzuna",
        external_id=str(item.get("id")),
        title=title,
        company=(item.get("company") or {}).get("display_name"),
        location=(item.get("location") or {}).get("display_name"),
        country=country.upper(),
        description=desc,
        url=item.get("redirect_url") or "",
        posted_at=item.get("created"),
        seniority=detect_seniority(title, desc),
        role_category=role_category,
        salary_min=item.get("salary_min"),
        salary_max=item.get("salary_max"),
        currency="EUR",  # DE and NL both EUR
        remote=bool(re.search(r"\bremote\b", f"{title} {desc[:500]}", re.I)),
        raw_json=json.dumps(item, ensure_ascii=False),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


# ----- 4. Arbeitnow (DE/NL/remote tech) -------------------------------------
def fetch_arbeitnow_all(queries: list[tuple[str, str]]) -> Iterator[Job]:
    """
    Arbeitnow's API has no query parameter, so we fetch ALL pages once
    and filter client-side across all queries. Default Python UA gets a 403,
    so we send browser-like headers.
    """
    url = "https://www.arbeitnow.com/api/job-board-api"
    queries_lower = [(q.lower(), role) for q, role in queries]
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.arbeitnow.com/",
    }
    target_locations = (
        "germany", "deutschland", "berlin", "münchen", "munich", "hamburg",
        "köln", "cologne", "frankfurt", "stuttgart",
        "netherlands", "nederland", "amsterdam", "rotterdam", "utrecht",
        "the hague", "eindhoven",
    )
    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        try:
            r = requests.get(url, params={"page": page}, headers=headers,
                             timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logging.warning(f"Arbeitnow page {page} failed: {e}")
            return
        jobs = data.get("data", [])
        if not jobs:
            return
        for item in jobs:
            title = (item.get("title") or "").lower()
            tags = " ".join(item.get("tags") or []).lower()
            desc_head = (item.get("description") or "")[:500].lower()
            loc = (item.get("location") or "").lower()
            is_remote = bool(item.get("remote")) or "remote" in loc
            in_target = any(c in loc for c in target_locations)
            if not (in_target or is_remote):
                continue
            for q_low, role in queries_lower:
                if q_low in title or q_low in tags or q_low in desc_head:
                    yield _parse_arbeitnow(item, role)
                    break
        if not (data.get("links") or {}).get("next"):
            return
        time.sleep(1.0)   # slower pacing — Arbeitnow rate-limits aggressively


def _parse_arbeitnow(item: dict, role_category: str) -> Job:
    loc = (item.get("location") or "")
    low = loc.lower()
    if any(x in low for x in ("germany", "deutschland", "berlin", "münchen",
                              "munich", "hamburg", "frankfurt", "köln", "stuttgart")):
        country = "DE"
    elif any(x in low for x in ("netherlands", "nederland", "amsterdam",
                                "rotterdam", "utrecht", "the hague", "eindhoven")):
        country = "NL"
    else:
        country = "REMOTE"
    posted = item.get("created_at")
    posted_iso = (
        datetime.fromtimestamp(posted, tz=timezone.utc).isoformat()
        if isinstance(posted, (int, float)) else None
    )
    slug = item.get("slug") or hashlib.md5((item.get("url") or "").encode()).hexdigest()
    return Job(
        source="arbeitnow",
        external_id=slug,
        title=item.get("title") or "",
        company=item.get("company_name"),
        location=loc or None,
        country=country,
        description=item.get("description"),
        url=item.get("url") or "",
        posted_at=posted_iso,
        seniority=detect_seniority(item.get("title") or "", item.get("description") or ""),
        role_category=role_category,
        salary_min=None, salary_max=None, currency=None,
        remote=bool(item.get("remote")),
        raw_json=json.dumps(item, ensure_ascii=False),
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
def _run_source(conn, source, country, query, role_category, generator) -> None:
    started = datetime.now(timezone.utc).isoformat()
    total = inserted = 0
    err = None
    try:
        for job in generator:
            total += 1
            if upsert_job(conn, job):
                inserted += 1
        conn.commit()
    except Exception as e:                       # noqa: BLE001
        err = str(e)
        logging.exception(f"{source}/{country}/{query} crashed")
    finally:
        conn.execute(
            """INSERT INTO fetch_runs (source, country, query, results_count,
               inserted_count, started_at, finished_at, error)
               VALUES (?,?,?,?,?,?,?,?)""",
            (source, country, query, total, inserted, started,
             datetime.now(timezone.utc).isoformat(), err),
        )
        conn.commit()
        logging.info(f"  {source:10s} {country:6s} '{query}'  fetched={total:4d}  new={inserted:4d}")


def run_collection() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    logging.info(f"DB: {DB_PATH.resolve()}")

    # Per-query sources: APIs that accept a search term
    for query, role in QUERIES:
        logging.info(f"--- '{query}' ({role}) ---")
        _run_source(conn, "jobtech",   "SE", query, role, fetch_jobtech(query, role))
        for c in ADZUNA_COUNTRIES:
            _run_source(conn, "adzuna", c.upper(), query, role,
                        fetch_adzuna(query, role, c))

    # Cross-query sources: feed-style APIs (we fetch once, match client-side)
    logging.info("--- cross-query sources ---")
    _run_source(conn, "nav",       "NO", "ALL", "mixed",
                fetch_nav_all(QUERIES))
    _run_source(conn, "arbeitnow", "EU", "ALL", "mixed",
                fetch_arbeitnow_all(QUERIES))

    conn.close()
    print(f"\n✔ Collection complete. DB: {DB_PATH.resolve()}")


# ----------------------------------------------------------------------------
# Reporting / export
# ----------------------------------------------------------------------------
def show_stats() -> None:
    conn = sqlite3.connect(DB_PATH)
    print(f"\nDatabase: {DB_PATH.resolve()}")
    total = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    print(f"Total jobs: {total}\n")

    print("By country × role × seniority:")
    rows = conn.execute("""
        SELECT country, role_category, seniority, COUNT(*) AS n
        FROM jobs
        GROUP BY country, role_category, seniority
        ORDER BY country, role_category, n DESC
    """).fetchall()
    print(f"  {'country':8s} {'role':18s} {'seniority':14s} {'count':>5s}")
    for c, r, s, n in rows:
        print(f"  {c or '-':8s} {r:18s} {s:14s} {n:5d}")

    print("\nTop 10 hiring companies (junior/mid only):")
    rows = conn.execute("""
        SELECT company, COUNT(*) AS n FROM jobs
        WHERE seniority IN ('junior','mid','unspecified')
          AND company IS NOT NULL
        GROUP BY company ORDER BY n DESC LIMIT 10
    """).fetchall()
    for company, n in rows:
        print(f"  {n:3d}  {company}")
    conn.close()


def export_csv(path: str = "jobs.csv") -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute("""
        SELECT source, country, role_category, seniority, title, company,
               location, posted_at, remote, salary_min, salary_max, currency, url
        FROM jobs ORDER BY posted_at DESC
    """)
    cols = [d[0] for d in cur.description]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(cur)
    conn.close()
    print(f"✔ Exported to {path}")


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stats", action="store_true", help="show DB stats only")
    parser.add_argument("--export", nargs="?", const="jobs.csv",
                        help="export to CSV (default: jobs.csv)")
    args = parser.parse_args()

    if args.stats:
        show_stats()
    elif args.export:
        export_csv(args.export)
    else:
        run_collection()
        show_stats()


if __name__ == "__main__":
    main()