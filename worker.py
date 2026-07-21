import asyncio
import concurrent.futures
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    aggregate_scores,
    analyze_sentiment_llm,
    analyze_sentiment_vader,
    fetch_appstore,
    fetch_lemmy,
    fetch_steam,
)
from workflows import ProductSentimentWorkflow

TASK_QUEUE = "sentiment-analysis-task-queue"


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect("localhost:7233")

    # The activities are synchronous and do blocking I/O (requests) or CPU work
    # (VADER). Running them on a ThreadPoolExecutor lets the workflow's fan-out
    # across sources actually execute in parallel, instead of serializing on the
    # asyncio event loop the way blocking calls in async activities would.
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
            client,
            task_queue=TASK_QUEUE,
            workflows=[ProductSentimentWorkflow],
            activities=[
                fetch_lemmy,
                fetch_steam,
                fetch_appstore,
                analyze_sentiment_vader,
                analyze_sentiment_llm,
                aggregate_scores,
            ],
            activity_executor=activity_executor,
        )
        print(f"Worker started, polling task queue {TASK_QUEUE!r}...")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
