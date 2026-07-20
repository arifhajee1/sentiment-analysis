# Architecture

```mermaid
flowchart LR
    CLI[starter.py CLI] -->|start / signal / query| TS[(Temporal Server\nlocalhost:7233)]
    TS <--> W[Worker\n+ ThreadPoolExecutor]
    W -->|runs| WF[ProductSentimentWorkflow]

    WF -->|Phase 1: fan-out\npartial-failure tolerant| F1[fetch_lemmy]
    WF --> F2[fetch_steam]
    WF --> F3[fetch_appstore]
    F1 -->|HTTP| L[(Lemmy search API)]
    F2 -->|HTTP| S[(Steam store + reviews API)]
    F3 -->|HTTP| A[(iTunes search + reviews RSS)]

    WF -->|Phase 2: fan-out\nover every review| SC[analyze_sentiment x N]
    SC -->|local| V[(VADER)]
    WF -->|Phase 4| AG[aggregate_scores]
```

Each source is an independent activity with its own retry policy and timeout, so
one source failing or being slow never blocks the others — the workflow
aggregates whatever succeeded and reports which sources failed. The activities
are synchronous and do blocking I/O, so the worker runs them on a
`ThreadPoolExecutor`; that's what lets the Phase 1 source fetches actually
execute in parallel rather than serializing on the asyncio event loop.

Temporal persists the workflow event history, so a worker crash mid-run resumes
without re-fetching the sources that already completed.
