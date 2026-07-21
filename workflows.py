import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import (
        aggregate_scores,
        analyze_sentiment_llm,
        analyze_sentiment_vader,
        fetch_appstore,
        fetch_lemmy,
        fetch_steam,
    )
    from shared import Review, ScoredReview

# Standard policy for a stable network dependency.
FETCH_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=4,
)

# Lemmy instances are community-run and can be slow, so back off more gently.
LEMMY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=4,
)

ANALYZE_RETRY_POLICY = RetryPolicy(maximum_attempts=3)

# Each source: (name, fetch activity, retry policy tuned to that dependency).
SOURCES = [
    ("lemmy", fetch_lemmy, LEMMY_RETRY_POLICY),
    ("steam", fetch_steam, FETCH_RETRY_POLICY),
    ("appstore", fetch_appstore, FETCH_RETRY_POLICY),
]

# After the initial batch is scored, how long to stay open for reviews signaled
# in by a caller before finalizing.
LATE_REVIEW_GRACE_PERIOD = timedelta(seconds=20)


@workflow.defn
class ProductSentimentWorkflow:
    def __init__(self) -> None:
        self._scored: list[ScoredReview] = []
        self._pending_extra: list[Review] = []
        self._failed_sources: list[str] = []
        self._closed = False

    @workflow.run
    async def run(self, query: str, max_reviews: int = 15) -> dict:
        # ---- Phase 1: fan out across sources, tolerant of partial failure ----
        reviews, self._failed_sources = await self._fetch_all_sources(query, max_reviews)
        if not reviews:
            raise ApplicationError(
                f"all review sources failed or returned nothing for {query!r}",
                non_retryable=True,
            )

        # ---- Phase 2: score the initial batch (fan-out over every review) ----
        await self._score_reviews(reviews)

        # ---- Phase 3: stay open briefly for reviews added via signal ----
        while not self._closed:
            try:
                await workflow.wait_condition(
                    lambda: bool(self._pending_extra) or self._closed,
                    timeout=LATE_REVIEW_GRACE_PERIOD,
                )
            except asyncio.TimeoutError:
                break
            if self._pending_extra:
                batch, self._pending_extra = self._pending_extra, []
                await self._score_reviews(batch)

        # ---- Phase 4: aggregate with per-source breakdown ----
        return await workflow.execute_activity(
            aggregate_scores,
            args=[self._scored, self._failed_sources],
            start_to_close_timeout=timedelta(seconds=10),
        )

    async def _fetch_all_sources(
        self, query: str, max_reviews: int
    ) -> tuple[list[Review], list[str]]:
        # return_exceptions=True is the crux: one source raising does not cancel
        # the others. A source that exhausts its retries lands as an exception in
        # the results list, and we record it as failed rather than aborting.
        results = await asyncio.gather(
            *(
                workflow.execute_activity(
                    fetch_fn,
                    args=[query, max_reviews],
                    start_to_close_timeout=timedelta(seconds=20),
                    # Cap total time incl. retries so one dead source can't stall
                    # the fan-in while the healthy ones have already returned.
                    schedule_to_close_timeout=timedelta(seconds=60),
                    retry_policy=retry_policy,
                )
                for _, fetch_fn, retry_policy in SOURCES
            ),
            return_exceptions=True,
        )

        reviews: list[Review] = []
        failed: list[str] = []
        for (name, _, _), result in zip(SOURCES, results):
            if isinstance(result, BaseException):
                failed.append(name)
            else:
                reviews.extend(result)
        return reviews, failed

    async def _score_reviews(self, reviews: list[Review]) -> None:
        # Score every review twice, concurrently: VADER (local, always succeeds)
        # and the LLM (network call, may fail per-review). return_exceptions on
        # the LLM fan-out lets a single failure fall back to None without losing
        # that review's VADER score or aborting the batch.
        vader_scores, llm_results = await asyncio.gather(
            asyncio.gather(*(
                workflow.execute_activity(
                    analyze_sentiment_vader,
                    r.text,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=ANALYZE_RETRY_POLICY,
                )
                for r in reviews
            )),
            asyncio.gather(
                *(
                    workflow.execute_activity(
                        analyze_sentiment_llm,
                        r.text,
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=ANALYZE_RETRY_POLICY,
                    )
                    for r in reviews
                ),
                return_exceptions=True,
            ),
        )
        self._scored.extend(
            ScoredReview(
                source=r.source,
                vader_score=v,
                llm_score=None if isinstance(llm, BaseException) else llm,
            )
            for r, v, llm in zip(reviews, vader_scores, llm_results)
        )

    @workflow.signal
    async def add_more_reviews(self, reviews: list[str]) -> None:
        self._pending_extra.extend(Review(text=t, source="manual") for t in reviews)

    @workflow.signal
    async def close_submission(self) -> None:
        self._closed = True

    @workflow.query
    def get_progress(self) -> dict:
        scored = len(self._scored)
        vader_average = (
            round(sum(sr.vader_score for sr in self._scored) / scored, 4) if scored else None
        )
        llm_scored = [sr.llm_score for sr in self._scored if sr.llm_score is not None]
        llm_average = (
            round(sum(llm_scored) / len(llm_scored), 4) if llm_scored else None
        )
        return {
            "reviews_scored": scored,
            "vader_running_average": vader_average,
            "llm_running_average": llm_average,
            "accepting_more": not self._closed,
            "failed_sources": self._failed_sources,
        }
