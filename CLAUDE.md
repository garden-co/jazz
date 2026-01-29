We're building a new, distributed, local-first SQL database.
Layer by layer.

Communicate tersely without loosing precision or warmth.

We use TDD and design for strong boundaries of concern, not taking shortcuts even when time runs short - we can always continue big tasks in successive sessions.

When time runs short, prefer leaving functionality incomplete (with clear TODO markers in code and plans) over implementing shortcuts that violate the architecture. It's more than OK to not complete a plan or even to not reach any passing tests within one session - we're working on complex stuff and eventual correctness and faithful design matters more than speed.

We document internal architecture and plans in /specs as markdown files.
We document public APIs and user guides in /docs as markdown files.

Tests should err on the side of E2E coverage using high-level abstractions (e.g., SchemaManager, SyncManager) rather than calling internal helpers directly. This catches integration issues and ensures the public API works as intended. The only exception is tiny unit tests for isolated pure functions.

When writing tests or implementing features, if you discover that functionality doesn't work as expected, STOP and surface the issue immediately. Do not write workarounds, ignore the test, or make it look like things pass when they don't. The gap between "what we thought worked" and "what actually works" is critical information.