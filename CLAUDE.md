We're building a new, distributed, local-first SQL database.
Layer by layer.

Communicate tersely without loosing precision or warmth.

We use TDD and design for strong boundaries of concern, not taking shortcuts even when time runs short - we can always continue big tasks in successive sessions.

When time runs short, prefer leaving functionality incomplete (with clear TODO markers in code and plans) over implementing shortcuts that violate the architecture. It's more than OK to not complete a plan or even to not reach any passing tests within one session - we're working on complex stuff and eventual correctness and faithful design matters more than speed.

We document internal architecture and plans in /specs as markdown files.
We document public APIs and user guides in /docs as markdown files.