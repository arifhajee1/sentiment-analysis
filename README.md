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
- **Phase 2 — score every review twice, in parallel.** Each review is scored by
  **two** engines concurrently: `analyze_sentiment_vader` (VADER, local, tuned for
  short informal text) and `analyze_sentiment_llm` (a Claude API call, defaulting
  to Haiku 4.5). VADER always produces a score; the LLM fan-out uses
  `return_exceptions=True`, so a failed or unavailable LLM score falls back to
  `None` without losing that review's VADER score. Reviews keep a `source` tag so
  both engines' scores roll up per source.
- **Phases 3–4 — stay open briefly for signaled reviews, then aggregate** into a
  side-by-side VADER-vs-LLM overall score, positive/neutral/negative labels, and a
  per-source breakdown showing both engines.

### The two scorers

Running both lets you compare a fast lexicon model against an LLM on the same
data — VADER is English-tuned and misses sarcasm, context, and other languages
(e.g. the non-English Lemmy comments), where the LLM does better. Because the LLM
call is a real, rate-limited network dependency (unlike local VADER), it's also
what makes the per-activity retry policy and `start_to_close_timeout` meaningful.
The LLM call is non-deterministic, but it lives in an **activity**, so its result
is recorded in history and replay reuses it — the workflow stays deterministic.

- **Model** — defaults to `claude-haiku-4-5`; override with the
  `SENTIMENT_LLM_MODEL` env var on the worker.
- **Auth** — set `ANTHROPIC_API_KEY` in the worker's environment. Without it, the
  LLM scorer fails fast (non-retryable) and every `llm_score` is `None`; the run
  still completes on VADER and the output shows the LLM column as `n/a`.

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
source fetches and the two Phase 2 scorers execute in true parallel rather than
serializing on the event loop.

## Running it

```bash
conda activate sentiment-analysis   # created via: conda create -n sentiment-analysis python=3.11
pip install -r requirements.txt

# terminal 1: local Temporal server (installed via `brew install temporal`)
temporal server start-dev --ui-port 8080

# terminal 2: worker (set ANTHROPIC_API_KEY to enable the LLM scorer)
ANTHROPIC_API_KEY=sk-... python worker.py

# terminal 3: kick off a run (try a game/app that exists on all three sources)
python starter.py "minecraft" --max-reviews 10 --watch
```

Example output (with an API key set — both engines populated):

```
Final result:
  review_count:    30
  VADER:  +0.2231  (positive)
  LLM:    +0.3400  (positive, 30/30 scored)
  by source:
    appstore   vader=+0.4777  llm=+0.5200  (10 reviews)
    lemmy      vader=-0.2634  llm=-0.1100  (10 reviews)
    steam      vader=+0.4550  llm=+0.5000  (10 reviews)
```

Without `ANTHROPIC_API_KEY`, the `LLM` line reads `unavailable` and each source's
`llm` column shows `n/a` — the run still completes on VADER.

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
