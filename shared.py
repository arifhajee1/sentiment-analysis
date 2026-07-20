from dataclasses import dataclass

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
    """A review's sentiment score, retaining its source for per-source rollups."""

    source: str
    score: float
