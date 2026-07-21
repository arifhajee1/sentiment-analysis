import html
import os
import re

import requests
from anthropic import Anthropic, AuthenticationError
from temporalio import activity
from temporalio.exceptions import ApplicationError
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

# LLM scorer config. Default to Haiku 4.5 — sentiment scoring is a small
# classification task, so the cheapest/fastest model is the right fit. The
# client is constructed lazily-tolerantly: if no ANTHROPIC_API_KEY is
# configured, `_llm_client` stays None and analyze_sentiment_llm fails fast
# (non-retryable), so the workflow still completes on VADER alone.
LLM_MODEL = os.environ.get("SENTIMENT_LLM_MODEL", "claude-haiku-4-5")
_LLM_SYSTEM = (
    "You score product-review sentiment. Given a review, respond with ONLY a "
    "single number from -1.0 (very negative) to 1.0 (very positive). No other text."
)
_num_re = re.compile(r"-?\d+(?:\.\d+)?")

try:
    _llm_client = Anthropic()
except Exception:  # noqa: BLE001 — e.g. no API key configured at all
    _llm_client = None


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
def analyze_sentiment_vader(review: str) -> float:
    return _analyzer.polarity_scores(review)["compound"]


@activity.defn
def analyze_sentiment_llm(review: str) -> float:
    if _llm_client is None:
        # No credentials at all — don't burn retries pretending it's transient.
        raise ApplicationError("ANTHROPIC_API_KEY is not configured", non_retryable=True)
    try:
        message = _llm_client.messages.create(
            model=LLM_MODEL,
            max_tokens=16,
            system=_LLM_SYSTEM,
            messages=[{"role": "user", "content": review}],
        )
    except AuthenticationError as exc:
        raise ApplicationError("invalid ANTHROPIC_API_KEY", non_retryable=True) from exc

    text = next(b.text for b in message.content if b.type == "text").strip()
    match = _num_re.search(text)
    if not match:
        # Malformed response — retryable; the model may comply on another attempt.
        raise ValueError(f"no numeric score in LLM response: {text!r}")
    return max(-1.0, min(1.0, float(match.group())))


def _classify(average: float) -> str:
    if average >= 0.05:
        return "positive"
    if average <= -0.05:
        return "negative"
    return "neutral"


def _engine_summary(scores: list[float]) -> dict:
    average = sum(scores) / len(scores)
    return {"overall_score": round(average, 4), "classification": _classify(average)}


@activity.defn
def aggregate_scores(scored: list[ScoredReview], failed_sources: list[str]) -> dict:
    if not scored:
        return {
            "review_count": 0,
            "vader": None,
            "llm": None,
            "by_source": {},
            "failed_sources": failed_sources,
        }

    vader_all = [sr.vader_score for sr in scored]
    llm_all = [sr.llm_score for sr in scored if sr.llm_score is not None]

    vader_summary = _engine_summary(vader_all)
    if llm_all:
        llm_summary = _engine_summary(llm_all)
        llm_summary["scored_count"] = len(llm_all)
    else:
        llm_summary = None  # LLM scorer unavailable for this run

    per_source: dict[str, dict[str, list[float]]] = {}
    for sr in scored:
        bucket = per_source.setdefault(sr.source, {"vader": [], "llm": []})
        bucket["vader"].append(sr.vader_score)
        if sr.llm_score is not None:
            bucket["llm"].append(sr.llm_score)

    by_source = {}
    for src, bucket in per_source.items():
        entry = {
            "count": len(bucket["vader"]),
            "vader_score": round(sum(bucket["vader"]) / len(bucket["vader"]), 4),
        }
        if bucket["llm"]:
            entry["llm_score"] = round(sum(bucket["llm"]) / len(bucket["llm"]), 4)
        by_source[src] = entry

    return {
        "review_count": len(scored),
        "vader": vader_summary,
        "llm": llm_summary,
        "by_source": by_source,
        "failed_sources": failed_sources,
    }
