# Workflow sequence

```mermaid
sequenceDiagram
    participant Client as starter.py
    participant WF as ProductSentimentWorkflow
    participant SRC as fetch_lemmy / fetch_steam / fetch_appstore (parallel)
    participant AN as analyze_sentiment (N, parallel)
    participant AG as aggregate_scores

    Client->>WF: start_workflow(query, max_reviews)

    par Phase 1 — fan out across sources (return_exceptions=True)
        WF->>SRC: execute_activity(fetch_lemmy)
        WF->>SRC: execute_activity(fetch_steam)
        WF->>SRC: execute_activity(fetch_appstore)
    end
    Note over WF,SRC: each source: own retry policy + timeout;<br/>a source that exhausts retries is recorded,<br/>not fatal — only all-sources-failed aborts
    SRC-->>WF: reviews[] + failed_sources[]

    par Phase 2 — fan out scoring over every review
        WF->>AN: execute_activity(analyze_sentiment, r1)
        WF->>AN: execute_activity(analyze_sentiment, rN)
    end
    AN-->>WF: compound scores (fan-in via asyncio.gather)

    opt Phase 3 — caller adds manual reviews
        Client->>WF: signal add_more_reviews([...])
        WF->>AN: score new batch (source="manual")
    end

    Client->>WF: query get_progress()
    WF-->>Client: {reviews_scored, running_average, failed_sources}

    Client->>WF: signal close_submission()  (or 20s grace timeout)
    WF->>AG: Phase 4 — execute_activity(aggregate_scores, scored, failed)
    AG-->>WF: {overall_score, classification, by_source, failed_sources}
    WF-->>Client: workflow result
```
