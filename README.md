## Home Swap Consumer Intelligence Tool

The home swapping category is growing, but the travelers who would benefit most from it are largely still booking Airbnbs and hotels by default. To understand why, and to identify where Kindred and other home swapping platforms have the clearest opportunity to reach them, I built a Reddit-powered consumer intelligence tool that continuously monitors the travel and remote-work communities where these decisions get discussed openly.

This project analyzes **2,104 posts** and **18,610 comments** across eight travel-related subreddits, using AI-powered tagging and thematic clustering to surface the trends, friction points, and unmet needs most relevant to Kindred's growth.

---

## How It Works

A three-stage pipeline:

**1. Reddit Scraper (`reddit_scraper.py`)**
Pulls posts and top comments from eight targeted subreddits (r/travel, r/digitalnomad, r/homeexchange, r/homeswap, r/remotework, r/solotravel, r/slowtravel, r/familytravel) using nine keywords including "home exchange," "slow travel," "digital nomad," "Kindred," and "house swap." All data is stored in a local SQLite database.

**2. Insight Extractor (`insight_extractor.py`)**
Passes each post and its top comments to Claude for structured analysis. Each post is tagged across six categories: friction complaints, unmet needs, competitor mentions, positive signals, product feedback, and community trends. A verbatim quote and one-sentence summary are extracted per post.

**3. Thematic Aggregation (`thematic_extract.py`)**
Clusters posts into actionable strategic themes by matching against summaries, topics, and verbatim quotes. For each theme, outputs the number of supporting posts, total upvotes, top quotes with Reddit source links, and a subreddit breakdown. The full dataset covers posts from mid-2025 through March 2026, representing roughly 91,000 upvotes and 31,000 comments of community signal.
