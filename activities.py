import html
import os
import re

import requests
from temporalio import activity
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from shared import Review, ScoredReview

# One activity per source. Each does its own real network I/O against a free,
# no-auth public API, and each returns a uniform list[Review] so the workflow
# can treat sources interchangeably. Activities are synchronous `def` functions
# using the blocking `requests` library; the worker runs them on a
# ThreadPoolExecutor, so the workflow's fan-out across sources executes truly
# in parallel (see worker.py).

# Lemmy (a federated, no-auth Reddit alternative) fronts some instances with
# Cloudflare, so we send a browser-like User-Agent.
LEMMY_INSTANCE = "https://lemmy.world"
HTTP_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

_analyzer = SentimentIntensityAnalyzer()
_tag_re = re.compile(r"<[^<]+?>")


def _clean(text: str) -> str:
    """Strip HTML tags/entities and trim; used on sources that return markup."""
    return html.unescape(_tag_re.sub(" ", text or "")).strip()


def _maybe_simulate_failure(source: str) -> None:
    """Demo hook: force the first attempt of the named source(s) to fail so the
    per-source retry policy (and partial-failure handling) can be shown live.
    e.g. SIMULATE_FLAKY_FETCH=reddit,steam
    """
    targets = {s.strip() for s in os.environ.get("SIMULATE_FLAKY_FETCH", "").split(",") if s.strip()}
    if source in targets and activity.info().attempt == 1:
        raise RuntimeError(f"simulated transient failure fetching {source}")


@activity.defn
def fetch_lemmy(query: str, max_reviews: int) -> list[Review]:
    _maybe_simulate_failure("lemmy")
    resp = requests.get(
        f"{LEMMY_INSTANCE}/api/v3/search",
        params={"q": query, "type_": "Comments", "sort": "TopAll", "limit": min(max_reviews, 50)},
        headers={"User-Agent": HTTP_UA},
        timeout=12,
    )
    resp.raise_for_status()
    comments = resp.json().get("comments", [])

    reviews = []
    for c in comments:
        text = _clean((c.get("comment") or {}).get("content", ""))
        if text:
            reviews.append(Review(text=text[:2000], source="lemmy"))
    return reviews


@activity.defn
def fetch_steam(query: str, max_reviews: int) -> list[Review]:
    _maybe_simulate_failure("steam")
    # Resolve the query to a Steam app id via the store search endpoint.
    search = requests.get(
        "https://store.steampowered.com/api/storesearch/",
        params={"term": query, "cc": "us", "l": "english"},
        timeout=10,
    )
    search.raise_for_status()
    items = search.json().get("items", [])
    if not items:
        return []  # no matching game -> this source simply contributes nothing
    app_id = items[0]["id"]

    resp = requests.get(
        f"https://store.steampowered.com/appreviews/{app_id}",
        params={
            "json": 1,
            "num_per_page": min(max_reviews, 100),
            "language": "english",
            "filter": "recent",
            "purchase_type": "all",
        },
        timeout=10,
    )
    resp.raise_for_status()
    reviews = []
    for r in resp.json().get("reviews", [])[:max_reviews]:
        text = (r.get("review") or "").strip()
        if text:
            reviews.append(Review(text=text[:2000], source="steam"))
    return reviews


@activity.defn
def fetch_appstore(query: str, max_reviews: int) -> list[Review]:
    _maybe_simulate_failure("appstore")
    # Resolve the query to an App Store app id via the iTunes Search API.
    search = requests.get(
        "https://itunes.apple.com/search",
        params={"term": query, "entity": "software", "limit": 1, "country": "us"},
        timeout=10,
    )
    search.raise_for_status()
    results = search.json().get("results", [])
    if not results:
        return []
    app_id = results[0]["trackId"]

    resp = requests.get(
        f"https://itunes.apple.com/us/rss/customerreviews/id={app_id}/sortby=mostrecent/json",
        timeout=10,
    )
    resp.raise_for_status()
    entries = resp.json().get("feed", {}).get("entry", [])

    reviews = []
    for entry in entries:
        # The first feed entry is app metadata (no rating); real reviews carry im:rating.
        if "im:rating" not in entry:
            continue
        text = (entry.get("content") or {}).get("label", "").strip()
        if text:
            reviews.append(Review(text=text[:2000], source="appstore"))
        if len(reviews) >= max_reviews:
            break
    return reviews


@activity.defn
def analyze_sentiment(review: str) -> float:
    return _analyzer.polarity_scores(review)["compound"]


@activity.defn
def aggregate_scores(scored: list[ScoredReview], failed_sources: list[str]) -> dict:
    if not scored:
        return {
            "overall_score": 0.0,
            "classification": "neutral",
            "review_count": 0,
            "by_source": {},
            "failed_sources": failed_sources,
        }

    per_source: dict[str, list[float]] = {}
    for sr in scored:
        per_source.setdefault(sr.source, []).append(sr.score)

    by_source = {
        src: {"score": round(sum(vals) / len(vals), 4), "count": len(vals)}
        for src, vals in per_source.items()
    }

    all_scores = [sr.score for sr in scored]
    average = sum(all_scores) / len(all_scores)
    if average >= 0.05:
        classification = "positive"
    elif average <= -0.05:
        classification = "negative"
    else:
        classification = "neutral"

    return {
        "overall_score": round(average, 4),
        "classification": classification,
        "review_count": len(scored),
        "by_source": by_source,
        "failed_sources": failed_sources,
    }
