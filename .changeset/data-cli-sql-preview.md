---
"jazz-tools": minor
---

Add preview `jazz-tools sql` and `schema tables`/`schema describe` commands that
compile a bounded SQL dialect onto the existing Jazz SDK query and mutation APIs.
SQL is never sent to the engine, and unsupported syntax fails before a session
opens.

- Data commands are machine-readable by default when stdout is not a terminal,
  and failures carry a stable error code with a documented exit code (0 success,
  2 usage, 3 credentials, 4 denied, 5 timeout, 6 row already exists).
- `schema` reads a local `schema.ts` by default and needs no server or
  credentials; `sql` uses the deployed schema unless `--schema-dir` is passed.
- New `--explain`, `--capabilities`, `--verbose`, `--id-seed`, and
  `--max-cell-width` flags. `--explain` validates a statement against the schema
  without connecting or executing.
- Writes report `atomic: false`; `INSERT ... WITH ID SEED` makes a repeat fail
  with `ALREADY_EXISTS` instead of writing a duplicate row.
- Removed the unreleased `jazz-tools data query` compatibility form in favour of
  `jazz-tools sql`.
