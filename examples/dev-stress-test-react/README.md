# Dev Stress Test App

This app is meant to support Jazz2 development to quickly test the performance of the DB under various scenarios.

What this app includes:

- `GenerateFixtures` action in `TodoList` to seed `15,000` projects and `15,000` todos in 1,000-row batches.
- `GenerateFixtures` action moved to a dedicated generator page, mounted independently from the todos query view.
- Content for generated fixtures now uses a fixed random-word pool for titles/descriptions instead of sequential names.
- Project-to-todo joins via the `projects` -> `todosViaProject` relation.
- Owner-based policies on both `projects` and `todos`.
