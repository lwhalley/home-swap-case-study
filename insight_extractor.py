"""
Insight extractor for Kindred consumer intelligence.
Reads posts + comments from SQLite, sends batches to Claude for analysis,
and stores structured insights back to the database.
"""

import sqlite3
import json
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from enum import Enum
from typing import Optional

import anthropic
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

MODEL           = "claude-opus-4-6"
BATCH_SIZE      = 20          # posts per Claude call
MAX_BODY_CHARS  = 1000        # truncate long post bodies
MAX_COMMENT_CHARS = 300       # truncate long comments
TOP_COMMENTS_PER_POST = 3     # comments to include per post for context
DB_PATH         = "kindred_insights.db"

SYSTEM_PROMPT = """You are a consumer insights analyst for Kindred, a premium home-swapping company.

You will receive a batch of Reddit posts (with their top comments) from travel and remote-work communities.

For EACH post, return a structured analysis with:

1. primary_topic — One concise sentence describing the main subject or question.

2. tags — One or more from this exact list (use all that apply):
   - "product_feedback"   : Users describing features or products they wish existed
   - "friction_complaints": Things that are broken, frustrating, or a barrier to action
   - "competitor_mentions": Any other service, platform, or brand is named (Airbnb, VRBO, HomeExchange, Behomm, etc.)
   - "positive_signals"   : Things users explicitly love, recommend, or are excited about
   - "unmet_needs"        : Desires or problems people cannot currently solve
   - "community_trends"   : Broader patterns in how people travel, work remotely, or live

3. quotes — 1–2 verbatim excerpts (from the post body or a comment) that best capture the insight.
   - Must be exact quotes, not paraphrased.
   - Prefer quotes that are specific, vivid, and actionable for a product team.

4. summary — 1–2 sentences synthesizing the insight and its relevance to home-swapping or slow travel.

Analyse every post in the batch. Do not skip any."""

# ── Pydantic schemas ──────────────────────────────────────────────────────────

class InsightTag(str, Enum):
    PRODUCT_FEEDBACK    = "product_feedback"
    FRICTION_COMPLAINTS = "friction_complaints"
    COMPETITOR_MENTIONS = "competitor_mentions"
    POSITIVE_SIGNALS    = "positive_signals"
    UNMET_NEEDS         = "unmet_needs"
    COMMUNITY_TRENDS    = "community_trends"


class PostInsight(BaseModel):
    post_id: str
    primary_topic: str
    tags: list[InsightTag]
    quotes: list[str]
    summary: str


class BatchInsights(BaseModel):
    insights: list[PostInsight]


# ── Database ──────────────────────────────────────────────────────────────────

def init_insights_table(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS insights (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id      TEXT NOT NULL REFERENCES posts(id),
            primary_topic TEXT,
            tags         TEXT,   -- JSON array of strings
            quotes       TEXT,   -- JSON array of strings
            summary      TEXT,
            raw_response TEXT,   -- full JSON from Claude
            analyzed_at  TEXT,
            UNIQUE(post_id)
        );
        CREATE INDEX IF NOT EXISTS idx_insights_post ON insights(post_id);
        CREATE INDEX IF NOT EXISTS idx_insights_tags ON insights(tags);
    """)
    conn.commit()


def fetch_unanalyzed_posts(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
) -> list[dict]:
    query = """
        SELECT p.id, p.subreddit, p.keyword, p.title, p.body,
               p.upvotes, p.num_comments, p.created_date
        FROM   posts p
        LEFT JOIN insights i ON i.post_id = p.id
        WHERE  i.post_id IS NULL
        ORDER  BY p.upvotes DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    cols = ["id", "subreddit", "keyword", "title", "body",
            "upvotes", "num_comments", "created_date"]
    return [dict(zip(cols, row)) for row in rows]


def fetch_comments_for_post(conn: sqlite3.Connection, post_id: str, n: int) -> list[str]:
    rows = conn.execute("""
        SELECT body FROM comments
        WHERE  post_id = ? AND body NOT IN ('[deleted]', '[removed]', '')
        ORDER  BY upvotes DESC
        LIMIT  ?
    """, (post_id, n)).fetchall()
    return [r[0] for r in rows]


def save_insights(conn: sqlite3.Connection, insights: list[PostInsight], raw_json: str):
    now = datetime.utcnow().isoformat()
    for insight in insights:
        conn.execute("""
            INSERT OR REPLACE INTO insights
                (post_id, primary_topic, tags, quotes, summary, raw_response, analyzed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            insight.post_id,
            insight.primary_topic,
            json.dumps([t.value for t in insight.tags]),
            json.dumps(insight.quotes),
            insight.summary,
            raw_json,
            now,
        ))
    conn.commit()


# ── Prompt builder ────────────────────────────────────────────────────────────

def build_user_message(posts: list[dict], conn: sqlite3.Connection) -> str:
    parts = []
    for i, post in enumerate(posts, 1):
        title   = (post["title"] or "").strip()
        body    = (post["body"]  or "").strip()[:MAX_BODY_CHARS]
        if len(post["body"] or "") > MAX_BODY_CHARS:
            body += "…"

        comments = fetch_comments_for_post(conn, post["id"], TOP_COMMENTS_PER_POST)
        comment_block = ""
        if comments:
            comment_lines = []
            for c in comments:
                c_text = c[:MAX_COMMENT_CHARS] + ("…" if len(c) > MAX_COMMENT_CHARS else "")
                comment_lines.append(f"  - {c_text}")
            comment_block = "\nTop comments:\n" + "\n".join(comment_lines)

        parts.append(
            f"--- POST {i} (id={post['id']}, r/{post['subreddit']}, "
            f"keyword={post['keyword']!r}, upvotes={post['upvotes']}) ---\n"
            f"Title: {title}\n"
            f"Body: {body or '[no body]'}"
            f"{comment_block}"
        )

    return (
        "Analyse each of the following Reddit posts and return the insights array.\n\n"
        + "\n\n".join(parts)
    )


# ── Claude call ───────────────────────────────────────────────────────────────

def analyse_batch(
    client: anthropic.Anthropic,
    posts: list[dict],
    conn: sqlite3.Connection,
) -> list[PostInsight]:
    user_message = build_user_message(posts, conn)

    # Use parse() for automatic Pydantic validation + structured output
    response = client.messages.parse(
        model=MODEL,
        max_tokens=4096,
        system=[{
            "type": "text",
            "text": SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},   # cache the system prompt
        }],
        messages=[{"role": "user", "content": user_message}],
        output_format=BatchInsights,
        thinking={"type": "adaptive"},
    )

    result: BatchInsights = response.parsed_output

    # Validate all post IDs are accounted for
    expected_ids = {p["id"] for p in posts}
    returned_ids = {i.post_id for i in result.insights}
    missing = expected_ids - returned_ids
    if missing:
        log.warning("Claude did not return insights for post IDs: %s", missing)

    return result.insights


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extract insights from Reddit posts using Claude."
    )
    parser.add_argument("--db",         default=DB_PATH,  help="SQLite database path")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--limit",      type=int, default=None,
                        help="Max posts to process (omit for all)")
    parser.add_argument("--api-key",    default=None,
                        help="Anthropic API key (or set ANTHROPIC_API_KEY env var)")
    args = parser.parse_args()

    client = anthropic.Anthropic(api_key=args.api_key) if args.api_key else anthropic.Anthropic()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")
    init_insights_table(conn)

    posts = fetch_unanalyzed_posts(conn, limit=args.limit)
    if not posts:
        log.info("No unanalyzed posts found — database is fully processed.")
        return

    total = len(posts)
    batches = [posts[i:i + args.batch_size] for i in range(0, total, args.batch_size)]
    log.info("Processing %d posts in %d batches (model: %s)", total, len(batches), MODEL)

    processed = 0
    errors = 0
    for batch_num, batch in enumerate(batches, 1):
        log.info("[%d/%d] Analysing batch of %d posts…", batch_num, len(batches), len(batch))
        try:
            insights = analyse_batch(client, batch, conn)
            raw_json = json.dumps([i.model_dump() for i in insights])
            save_insights(conn, insights, raw_json)
            processed += len(insights)
            log.info("  -> saved %d insights (total so far: %d)", len(insights), processed)
        except anthropic.RateLimitError as e:
            retry_after = int(getattr(e.response, "headers", {}).get("retry-after", 60))
            log.warning("Rate limited — sleeping %ds", retry_after)
            time.sleep(retry_after)
            batch_num -= 1   # retry this batch
        except anthropic.APIStatusError as e:
            log.error("API error on batch %d: %s", batch_num, e)
            errors += 1
            if errors >= 5:
                log.error("Too many consecutive errors — stopping.")
                break
        except Exception as e:
            log.error("Unexpected error on batch %d: %s", batch_num, e)
            errors += 1

    # Summary
    total_insights = conn.execute("SELECT COUNT(*) FROM insights").fetchone()[0]
    log.info(
        "Done — %d new insights written | %d total in DB | %d errors",
        processed, total_insights, errors,
    )
    log.info("Database: %s", Path(args.db).resolve())
    conn.close()


if __name__ == "__main__":
    main()
