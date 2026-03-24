"""
Reddit scraper for Kindred consumer insights & competitive intelligence.
Uses the Arctic Shift archive API (no credentials required).
https://arctic-shift.photon-reddit.com
"""

import sqlite3
import time
import logging
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://arctic-shift.photon-reddit.com/api"

DEFAULT_SUBREDDITS = [
    "digitalnomad",
    "slowtravel",
    "travel",
    "solotravel",
    "homeswap",
    "familytravel",
    "remotework",
    "homeexchange",
]

DEFAULT_KEYWORDS = [
    "Kindred",
    "home exchange",
    "house swap",
    "home swap",
    "Behomm",
    "HomeExchange",
    "slow travel",
    "work remotely abroad",
    "digital nomad",
    "house sitting",
    "traveling with pet",
    "traveling with family",
    "traveling with kids",
]

POSTS_PER_KEYWORD   = 500          # target posts per keyword/subreddit combo
TOP_COMMENTS        = 20           # top comments to fetch per post
LOOKBACK_MONTHS     = 12
PAGE_SIZE           = 100          # Arctic Shift max per request
PAUSE_BETWEEN_REQS  = 0.6          # seconds — stay well under rate limit
DB_PATH             = "kindred_insights.db"

# ── Database ──────────────────────────────────────────────────────────────────

def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            id            TEXT PRIMARY KEY,
            subreddit     TEXT NOT NULL,
            keyword       TEXT NOT NULL,
            title         TEXT,
            body          TEXT,
            url           TEXT,
            author        TEXT,
            upvotes       INTEGER,
            num_comments  INTEGER,
            created_utc   REAL,
            created_date  TEXT,
            scraped_at    TEXT
        );

        CREATE TABLE IF NOT EXISTS comments (
            id            TEXT PRIMARY KEY,
            post_id       TEXT NOT NULL REFERENCES posts(id),
            body          TEXT,
            author        TEXT,
            upvotes       INTEGER,
            created_utc   REAL,
            created_date  TEXT,
            scraped_at    TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
        CREATE INDEX IF NOT EXISTS idx_posts_keyword   ON posts(keyword);
        CREATE INDEX IF NOT EXISTS idx_comments_post   ON comments(post_id);
    """)
    conn.commit()
    return conn


def upsert_post(conn, row: dict):
    conn.execute("""
        INSERT OR IGNORE INTO posts
            (id, subreddit, keyword, title, body, url, author,
             upvotes, num_comments, created_utc, created_date, scraped_at)
        VALUES
            (:id, :subreddit, :keyword, :title, :body, :url, :author,
             :upvotes, :num_comments, :created_utc, :created_date, :scraped_at)
    """, row)


def upsert_comment(conn, row: dict):
    conn.execute("""
        INSERT OR IGNORE INTO comments
            (id, post_id, body, author, upvotes, created_utc, created_date, scraped_at)
        VALUES
            (:id, :post_id, :body, :author, :upvotes, :created_utc, :created_date, :scraped_at)
    """, row)


def post_exists(conn, post_id: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM posts WHERE id=?", (post_id,)
    ).fetchone() is not None


def comments_fetched(conn, post_id: str) -> bool:
    return conn.execute(
        "SELECT COUNT(*) FROM comments WHERE post_id=?", (post_id,)
    ).fetchone()[0] > 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def ts_to_iso(ts: float) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def cutoff_ts(months: int) -> int:
    return int((datetime.utcnow() - timedelta(days=months * 30)).timestamp())


def get_json(session: requests.Session, endpoint: str, params: dict) -> dict | None:
    """GET request with retry on transient errors."""
    url = f"{BASE_URL}/{endpoint}"
    for attempt in range(3):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = int(resp.headers.get("X-RateLimit-Reset", 60))
                log.warning("Rate limited — sleeping %ds", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            log.warning("Request error (attempt %d/3): %s", attempt + 1, e)
            time.sleep(5 * (attempt + 1))
    log.error("Failed after 3 attempts: %s %s", endpoint, params)
    return None


# ── Comment fetcher ───────────────────────────────────────────────────────────

def fetch_top_comments(session: requests.Session, post_id: str, top_n: int) -> list[dict]:
    """Fetch comments for a post, sorted by score descending."""
    params = {
        "link_id": post_id,
        "limit": min(top_n * 3, 100),   # fetch extra, then trim by score
        "sort": "desc",
        "fields": "id,link_id,body,author,score,created_utc",
    }
    data = get_json(session, "comments/search", params)
    if not data or "data" not in data:
        return []

    now_iso = datetime.utcnow().isoformat()
    comments = sorted(data["data"], key=lambda c: c.get("score", 0), reverse=True)[:top_n]

    results = []
    for c in comments:
        body = c.get("body", "")
        if not body or body in ("[deleted]", "[removed]"):
            continue
        results.append({
            "id": c["id"],
            "post_id": post_id,
            "body": body,
            "author": c.get("author", "[deleted]"),
            "upvotes": c.get("score", 0),
            "created_utc": float(c.get("created_utc", 0)),
            "created_date": ts_to_iso(float(c.get("created_utc", 0))),
            "scraped_at": now_iso,
        })
    return results


# ── Post fetcher ──────────────────────────────────────────────────────────────

def scrape_subreddit_keyword(
    session: requests.Session,
    conn: sqlite3.Connection,
    subreddit: str,
    keyword: str,
    after_ts: int,
    target: int,
    top_comments: int,
):
    log.info("  r/%s | '%s'", subreddit, keyword)
    now_iso = datetime.utcnow().isoformat()
    collected = skipped_dup = 0

    # Paginate by walking backwards in time using `before`
    before_ts = int(datetime.utcnow().timestamp())
    fetched_ids: set[str] = set()

    while collected < target:
        params = {
            "query": keyword,
            "subreddit": subreddit,
            "after": after_ts,
            "before": before_ts,
            "limit": PAGE_SIZE,
            "sort": "desc",
            "fields": "id,subreddit,title,selftext,url,author,score,num_comments,created_utc",
        }
        data = get_json(session, "posts/search", params)
        if not data or not data.get("data"):
            break

        posts = data["data"]
        if not posts:
            break

        new_before = None
        for p in posts:
            pid = p.get("id", "")
            if not pid or pid in fetched_ids:
                continue
            fetched_ids.add(pid)
            new_before = min(new_before or float("inf"), float(p.get("created_utc", 0)))

            if post_exists(conn, pid):
                skipped_dup += 1
                if not comments_fetched(conn, pid):
                    cmts = fetch_top_comments(session, pid, top_comments)
                    for c in cmts:
                        upsert_comment(conn, c)
                    conn.commit()
                    time.sleep(PAUSE_BETWEEN_REQS)
                continue

            created_utc = float(p.get("created_utc", 0))
            post_row = {
                "id": pid,
                "subreddit": subreddit,
                "keyword": keyword,
                "title": p.get("title", ""),
                "body": p.get("selftext", "") or "",
                "url": p.get("url", ""),
                "author": p.get("author", "[deleted]"),
                "upvotes": p.get("score", 0),
                "num_comments": p.get("num_comments", 0),
                "created_utc": created_utc,
                "created_date": ts_to_iso(created_utc),
                "scraped_at": now_iso,
            }
            upsert_post(conn, post_row)

            cmts = fetch_top_comments(session, pid, top_comments)
            for c in cmts:
                upsert_comment(conn, c)

            conn.commit()
            collected += 1
            time.sleep(PAUSE_BETWEEN_REQS)

            if collected >= target:
                break

        # Move window back in time for next page
        if new_before is None or new_before <= after_ts:
            break
        before_ts = int(new_before) - 1

        # If the page returned fewer than PAGE_SIZE results, we've exhausted the query
        if len(posts) < PAGE_SIZE:
            break

        time.sleep(PAUSE_BETWEEN_REQS)

    log.info("    -> new=%d  dupes=%d", collected, skipped_dup)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Reddit via Arctic Shift for Kindred consumer insights."
    )
    parser.add_argument("--keywords",        nargs="+", default=DEFAULT_KEYWORDS)
    parser.add_argument("--subreddits",      nargs="+", default=DEFAULT_SUBREDDITS)
    parser.add_argument("--posts-per-kw",    type=int,  default=POSTS_PER_KEYWORD)
    parser.add_argument("--top-comments",    type=int,  default=TOP_COMMENTS)
    parser.add_argument("--lookback-months", type=int,  default=LOOKBACK_MONTHS)
    parser.add_argument("--db",              default=DB_PATH)
    args = parser.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = "kindred-insights-scraper/1.0"

    conn = init_db(args.db)
    after = cutoff_ts(args.lookback_months)

    total = len(args.subreddits) * len(args.keywords)
    log.info(
        "Starting — %d subreddits × %d keywords = %d combos",
        len(args.subreddits), len(args.keywords), total,
    )
    log.info(
        "Lookback: %d months | Posts/kw: %d | Top comments: %d | DB: %s",
        args.lookback_months, args.posts_per_kw, args.top_comments,
        Path(args.db).resolve(),
    )

    done = 0
    for sub in args.subreddits:
        for kw in args.keywords:
            done += 1
            log.info("[%d/%d]", done, total)
            scrape_subreddit_keyword(
                session, conn, sub, kw,
                after, args.posts_per_kw, args.top_comments,
            )

    post_count    = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    comment_count = conn.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    log.info("Done — %d posts, %d comments → %s",
             post_count, comment_count, Path(args.db).resolve())
    conn.close()


if __name__ == "__main__":
    main()
