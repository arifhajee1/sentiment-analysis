import argparse
import asyncio
import uuid

from temporalio.client import Client

from workflows import ProductSentimentWorkflow

TASK_QUEUE = "sentiment-analysis-task-queue"


def print_result(result: dict) -> None:
    print("Final result:")
    print(f"  review_count:    {result['review_count']}")

    vader = result.get("vader")
    if vader:
        print(f"  VADER:  {vader['overall_score']:+.4f}  ({vader['classification']})")

    llm = result.get("llm")
    if llm:
        print(
            f"  LLM:    {llm['overall_score']:+.4f}  ({llm['classification']}, "
            f"{llm['scored_count']}/{result['review_count']} scored)"
        )
    else:
        print("  LLM:    unavailable (set ANTHROPIC_API_KEY to enable)")

    if result.get("by_source"):
        print("  by source:")
        for src, info in result["by_source"].items():
            if "llm_score" in info:
                llm_col = f"llm={info['llm_score']:+.4f}"
            else:
                llm_col = "llm=  n/a  "
            print(
                f"    {src:10s} vader={info['vader_score']:+.4f}  {llm_col}  "
                f"({info['count']} reviews)"
            )
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
