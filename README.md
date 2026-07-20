# Product Sentiment Analysis (Temporal exercise)

A Temporal workflow that answers: *"what's the overall sentiment for this
product, across the places people actually talk about it?"* It fans out across
**three independent public sources** in parallel, scores every review it gathers
for sentiment, and aggregates them into an overall score **plus a per-source
breakdown** — while tolerating any individual source failing.

See [`diagrams/architecture.md`](diagrams/architecture.md) and
[`diagrams/sequence.md`](diagrams/sequence.md) for how the pieces fit together.

## The three sources

All are free, public, and require no auth or API key, so `fetch_*` are genuine
network-bound I/O calls subject to real latency and failure — the point being to
exercise Temporal, not to fight bot walls:

| Source | Endpoint | What it contributes |
|---|---|---|
| **Lemmy** | `lemmy.world/api/v3/search` | Federated forum comments (Reddit-style chatter) |
| **Steam** | `store.steampowered.com/appreviews` | Game reviews |
| **App Store** | `itunes.apple.com/.../customerreviews` | Mobile app reviews |

> **Why Lemmy instead of Reddit?** Reddit now IP-blocks unauthenticated traffic
> (hard `403` regardless of User-Agent), so its free JSON endpoint can't be used
> without registering a Reddit OAuth app — exactly the kind of live-auth
> integration this exercise says to skip. Lemmy is the federated, no-auth
> equivalent and fills the same "social chatter" role.

Each source resolves the query itself (Steam/App Store look up an app id first),
so one query string like `"minecraft"` fans out to all three.

## Design

The workflow is a **two-phase fan-out**:

- **Phase 1 — fetch across sources, tolerant of partial failure.** Each source is
  its own activity with a retry policy tuned to it (Lemmy backs off more gently).
  They run concurrently via `asyncio.gather(..., return_exceptions=True)`: a
  source that exhausts its retries lands as an exception in the results list and
  is recorded in `failed_sources` instead of aborting the run. The workflow only
  fails if **every** source fails. A `schedule_to_close_timeout` caps the total
  time (including retries) any one source can spend, so a dead source can't stall
  the fan-in while the healthy ones have already returned.
- **Phase 2 — score every review, in parallel.** Another `gather`, one
  `analyze_sentiment` activity per review (VADER, tuned for short informal text).
  Reviews keep a `source` tag so scores can be rolled up per source.
- **Phases 3–4 — stay open briefly for signaled reviews, then aggregate** into an
  overall score, a positive/neutral/negative label, and a per-source breakdown.

Other Temporal features on show:

- **`add_more_reviews` signal** — inject extra review text into a *running*
  workflow; it's scored and folded in as a `manual` source.
- **`close_submission` signal** — finalize immediately instead of waiting out the
  grace period.
- **`get_progress` query** — read a running workflow's partial state (reviews
  scored, running average, which sources failed) without touching a database.

All network and model I/O lives in activities; the workflow only orchestrates,
which is what keeps it replay-deterministic. The activities are synchronous and
run on a `ThreadPoolExecutor` (see [`worker.py`](worker.py)) so the Phase 1
source fetches execute in true parallel rather than serializing on the event
loop.

## Running it

```bash
conda activate sentiment-analysis   # created via: conda create -n sentiment-analysis python=3.11
pip install -r requirements.txt

# terminal 1: local Temporal server (installed via `brew install temporal`)
temporal server start-dev --ui-port 8080

# terminal 2: worker
python worker.py

# terminal 3: kick off a run (try a game/app that exists on all three sources)
python starter.py "minecraft" --max-reviews 10 --watch
```

Example output:

```
Final result:
  overall_score:   +0.2231
  classification:  positive
  review_count:    30
  by source:
    appstore   score=+0.4777  (10 reviews)
    lemmy      score=-0.2634  (10 reviews)
    steam      score=+0.4550  (10 reviews)
```

### Demoing partial failure + retries

```bash
# Force one or more sources to fail their first attempt; Temporal retries and recovers.
SIMULATE_FLAKY_FETCH=steam python worker.py
```

`SIMULATE_FLAKY_FETCH` takes a comma-separated list of source names
(`lemmy,steam,appstore`). Each named source raises on its first attempt only, so
you can watch the retry policy recover it in the [Temporal Web
UI](http://localhost:8080) event history. If a source is genuinely unreachable,
it lands in `failed_sources` and the run still completes on the others.

### Demoing signals

While a `--watch` run is still open (it stays open for a 20s grace period after
the initial batch is scored):

```bash
temporal workflow signal --workflow-id <id-from-starter-output> \
  --name add_more_reviews --input '["this product is amazing, highly recommend"]'

temporal workflow signal --workflow-id <id-from-starter-output> --name close_submission
```

## Scope notes

Per the exercise FAQ, this intentionally skips production concerns
(containerization, CI/CD, deployment manifests) — it's a local-run demonstration
of Temporal concepts, not a production service.
