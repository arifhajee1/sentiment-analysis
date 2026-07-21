from dataclasses import dataclass
from typing import Optional

# Shared data types passed across the workflow <-> activity boundary. Kept in
# their own module (no heavy imports, no side effects) so both the workflow
# sandbox and the activities can import them cheaply, and so Temporal's data
# converter has a stable, annotated type to (de)serialize.


@dataclass
class Review:
    """One piece of text to be scored, tagged with where it came from."""

    text: str
    source: str


@dataclass
class ScoredReview:
    """A review scored by both engines, retaining its source for per-source
    rollups. `llm_score` is None when the LLM scorer was unavailable or failed
    for this review (VADER is local and always produces a score)."""

    source: str
    vader_score: float
    llm_score: Optional[float]
