import argparse
import asyncio
import uuid

from temporalio.client import Client

from workflows import ProductSentimentWorkflow

TASK_QUEUE = "sentiment-analysis-task-queue"


def print_result(result: dict) -> None:
    print("Final result:")
    print(f"  overall_score:   {result['overall_score']:+.4f}")
    print(f"  classification:  {result['classification']}")
    print(f"  review_count:    {result['review_count']}")
    if result.get("by_source"):
        print("  by source:")
        for src, info in result["by_source"].items():
            print(f"    {src:10s} score={info['score']:+.4f}  ({info['count']} reviews)")
    if result.get("failed_sources"):
        print(f"  failed sources:  {', '.join(result['failed_sources'])}")


async def main(query: str, max_reviews: int, watch: bool) -> None:
    client = await Client.connect("localhost:7233")

    handle = await client.start_workflow(
        ProductSentimentWorkflow.run,
        args=[query, max_reviews],
        id=f"sentiment-{query.replace(' ', '_')}-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    print(f"Started workflow {handle.id!r} for query {query!r}")

    if watch:
        result_task = asyncio.ensure_future(handle.result())
        while not result_task.done():
            progress = await handle.query(ProductSentimentWorkflow.get_progress)
            print(f"  progress: {progress}")
            await asyncio.wait([result_task], timeout=2)
        result = result_task.result()
    else:
        result = await handle.result()

    print_result(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a product sentiment analysis workflow.")
    parser.add_argument("query", help="Product/topic to search each source for")
    parser.add_argument(
        "--max-reviews", type=int, default=15, help="Cap on reviews fetched per source"
    )
    parser.add_argument(
        "--watch", action="store_true", help="Poll and print progress via the get_progress query"
    )
    args = parser.parse_args()

    asyncio.run(main(args.query, args.max_reviews, args.watch))
