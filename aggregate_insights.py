"""
Aggregates insights from the Kindred intelligence database.
Outputs:
  - insights_summary.csv    — all aggregation tables in one file
  - top_quotes.json         — top 5 verbatim quotes per category
"""

import sqlite3
import json
import csv
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DB_PATH      = "kindred_insights.db"
CSV_OUT      = "insights_summary.csv"
QUOTES_OUT   = "top_quotes.json"

TAGS = [
    "product_feedback",
    "friction_complaints",
    "competitor_mentions",
    "positive_signals",
    "unmet_needs",
    "community_trends",
]

KINDRED_KEYWORDS   = {"kindred"}
HOME_EXCHANGE_KEYWORDS = {
    "home exchange", "house swap", "home swap", "homeexchange",
    "behomm", "house sitting",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_enriched_posts(conn: sqlite3.Connection) -> list[dict]:
    """Join posts + insights into one flat record per post."""
    rows = conn.execute("""
        SELECT
            p.id, p.subreddit, p.keyword, p.title, p.upvotes,
            p.created_date,
            i.primary_topic, i.tags, i.quotes, i.summary
        FROM insights i
        JOIN posts p ON p.id = i.post_id
        WHERE i.tags IS NOT NULL
          AND p.subreddit != 'digitalnomad'
    """).fetchall()

    cols = ["id", "subreddit", "keyword", "title", "upvotes",
            "created_date", "primary_topic", "tags_json", "quotes_json", "summary"]
    records = []
    for row in rows:
        r = dict(zip(cols, row))
        r["tags"]   = json.loads(r.pop("tags_json") or "[]")
        r["quotes"] = json.loads(r.pop("quotes_json") or "[]")
        try:
            r["month"] = r["created_date"][:7]   # "YYYY-MM"
        except Exception:
            r["month"] = "unknown"
        records.append(r)
    return records


def write_csv_section(writer: csv.writer, title: str, headers: list, rows: list):
    writer.writerow([])
    writer.writerow([f"=== {title} ==="])
    writer.writerow(headers)
    for row in rows:
        writer.writerow(row)


# ── Analysis functions ────────────────────────────────────────────────────────

def tag_frequency_by_month(records: list[dict]) -> list[tuple]:
    """Count tag occurrences per month."""
    counts: dict[tuple, int] = defaultdict(int)
    for r in records:
        for tag in r["tags"]:
            counts[(r["month"], tag)] += 1

    rows = sorted(counts.items(), key=lambda x: (x[0][0], x[0][1]))
    return [(month, tag, count) for (month, tag), count in rows]


def most_common_unmet_needs(records: list[dict], top_n: int = 20) -> list[tuple]:
    """Top topics where tag == 'unmet_needs', ranked by total upvotes."""
    topic_upvotes: dict[str, int]  = defaultdict(int)
    topic_count:   dict[str, int]  = defaultdict(int)

    for r in records:
        if "unmet_needs" in r["tags"]:
            topic = r["primary_topic"]
            topic_upvotes[topic] += r["upvotes"] or 0
            topic_count[topic]   += 1

    rows = sorted(topic_upvotes.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [(topic, topic_upvotes[topic], topic_count[topic]) for topic, _ in rows]


def competitor_frequency(records: list[dict]) -> list[tuple]:
    """Posts tagged competitor_mentions — extract named competitors from titles/topics."""
    KNOWN_COMPETITORS = [
        "airbnb", "vrbo", "homeexchange", "home exchange", "behomm",
        "trusted housesitters", "house sitting", "couchsurfing",
        "booking.com", "hipcamp", "kindred",
    ]

    comp_counts:   dict[str, int] = defaultdict(int)
    comp_upvotes:  dict[str, int] = defaultdict(int)

    for r in records:
        if "competitor_mentions" not in r["tags"]:
            continue
        text = (r["title"] + " " + r["primary_topic"] + " " + r["summary"]).lower()
        for comp in KNOWN_COMPETITORS:
            if comp in text:
                comp_counts[comp]  += 1
                comp_upvotes[comp] += r["upvotes"] or 0

    rows = sorted(comp_counts.items(), key=lambda x: x[1], reverse=True)
    return [(comp, comp_counts[comp], comp_upvotes[comp]) for comp, _ in rows]


def top_posts_per_tag(records: list[dict], top_n: int = 5) -> list[tuple]:
    """Highest-upvoted post per category tag."""
    tag_posts: dict[str, list] = defaultdict(list)
    for r in records:
        for tag in r["tags"]:
            tag_posts[tag].append(r)

    rows = []
    for tag in TAGS:
        sorted_posts = sorted(tag_posts.get(tag, []), key=lambda x: x["upvotes"] or 0, reverse=True)
        for rank, p in enumerate(sorted_posts[:top_n], 1):
            rows.append((tag, rank, p["upvotes"], p["subreddit"], p["title"][:120], p["summary"]))
    return rows


def kindred_vs_category(records: list[dict]) -> list[tuple]:
    """Side-by-side: Kindred-specific posts vs. broader home-exchange posts."""
    kindred_tags:  dict[str, int] = defaultdict(int)
    category_tags: dict[str, int] = defaultdict(int)
    kindred_total  = 0
    category_total = 0

    for r in records:
        kw = r["keyword"].lower()
        is_kindred   = kw in KINDRED_KEYWORDS
        is_category  = kw in HOME_EXCHANGE_KEYWORDS

        if not is_kindred and not is_category:
            continue

        for tag in r["tags"]:
            if is_kindred:
                kindred_tags[tag]  += 1
                kindred_total      += 1
            if is_category:
                category_tags[tag] += 1
                category_total     += 1

    rows = []
    for tag in TAGS:
        k_count = kindred_tags.get(tag, 0)
        c_count = category_tags.get(tag, 0)
        k_pct   = round(100 * k_count / kindred_total, 1)  if kindred_total  else 0
        c_pct   = round(100 * c_count / category_total, 1) if category_total else 0
        rows.append((tag, k_count, k_pct, c_count, c_pct))
    return rows


def top_quotes_per_tag(records: list[dict], top_n: int = 5) -> dict[str, list[dict]]:
    """Top N quotes per tag, ranked by post upvotes as a proxy for resonance."""
    tag_quotes: dict[str, list[dict]] = defaultdict(list)

    for r in records:
        for tag in r["tags"]:
            for quote in r["quotes"]:
                if quote and len(quote.strip()) > 20:
                    tag_quotes[tag].append({
                        "quote":     quote.strip(),
                        "post_title": r["title"],
                        "subreddit": r["subreddit"],
                        "upvotes":   r["upvotes"] or 0,
                        "summary":   r["summary"],
                    })

    result = {}
    for tag in TAGS:
        ranked = sorted(tag_quotes.get(tag, []), key=lambda x: x["upvotes"], reverse=True)
        # Deduplicate by quote text
        seen: set[str] = set()
        unique = []
        for q in ranked:
            key = q["quote"][:80]
            if key not in seen:
                seen.add(key)
                unique.append(q)
            if len(unique) >= top_n:
                break
        result[tag] = unique

    return result


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Aggregate Kindred insights into CSV and JSON reports."
    )
    parser.add_argument("--db",     default=DB_PATH)
    parser.add_argument("--csv",    default=CSV_OUT)
    parser.add_argument("--quotes", default=QUOTES_OUT)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    records = load_enriched_posts(conn)
    conn.close()

    if not records:
        print("No analysed posts found. Run insight_extractor.py first.")
        return

    print(f"Loaded {len(records)} analysed posts.")

    # ── Run analyses ──────────────────────────────────────────────────────────
    monthly      = tag_frequency_by_month(records)
    unmet        = most_common_unmet_needs(records)
    competitors  = competitor_frequency(records)
    top_posts    = top_posts_per_tag(records)
    comparison   = kindred_vs_category(records)
    quotes_data  = top_quotes_per_tag(records)

    # ── Write CSV ─────────────────────────────────────────────────────────────
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([f"Kindred Consumer Intelligence Report — generated {datetime.utcnow().strftime('%Y-%m-%d')}"])

        write_csv_section(w,
            "1. Tag Frequency by Month",
            ["month", "tag", "post_count"],
            monthly,
        )

        write_csv_section(w,
            "2. Most Common Unmet Needs (ranked by total upvotes)",
            ["primary_topic", "total_upvotes", "post_count"],
            unmet,
        )

        write_csv_section(w,
            "3. Competitor Mention Frequency",
            ["competitor", "mention_count", "total_upvotes"],
            competitors,
        )

        write_csv_section(w,
            "4. Top-Upvoted Posts per Category",
            ["tag", "rank", "upvotes", "subreddit", "title", "summary"],
            top_posts,
        )

        write_csv_section(w,
            "5. Kindred vs. Home Exchange Category (tag distribution)",
            ["tag", "kindred_count", "kindred_%", "category_count", "category_%"],
            comparison,
        )

    # ── Write quotes JSON ─────────────────────────────────────────────────────
    output = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_posts_analysed": len(records),
        "top_quotes_per_category": quotes_data,
    }
    with open(args.quotes, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # ── Print summary to terminal ─────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  CSV report  →  {Path(args.csv).resolve()}")
    print(f"  Quotes JSON →  {Path(args.quotes).resolve()}")
    print(f"{'─'*60}\n")

    print("TAG TOTALS:")
    tag_totals: dict[str, int] = defaultdict(int)
    for r in records:
        for t in r["tags"]:
            tag_totals[t] += 1
    for tag in TAGS:
        print(f"  {tag:<25} {tag_totals.get(tag, 0):>5} posts")

    print("\nTOP 5 UNMET NEEDS:")
    for topic, upvotes, count in unmet[:5]:
        print(f"  [{count} posts, {upvotes} upvotes] {topic[:80]}")

    print("\nCOMPETITOR MENTIONS:")
    for comp, count, upvotes in competitors[:8]:
        print(f"  {comp:<25} {count:>4} mentions  ({upvotes} upvotes)")

    print()


if __name__ == "__main__":
    main()
