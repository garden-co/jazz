# Testing Jazz applications

Prefer tests that assert user-visible rows, subscription deliveries, or accepted/rejected writes
through public APIs. Model the actual topology when authorization, offline behavior, or sync is
part of the behavior under test. Keep a test's requested durability tier aligned with its
assertion.

- [Testing recipe](https://jazz.tools/docs/recipes/testing)
- [Real-time collaborative list recipe](https://jazz.tools/docs/recipes/data-patterns/real-time-collaborative-list)
- [Nested data recipe](https://jazz.tools/docs/recipes/data-patterns/nested-data)
