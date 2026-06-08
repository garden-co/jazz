# sessionLogTable.test.mjs Tests

This suite covers the local telemetry session explorer model, routing, SQL generation, and source-level UI constraints.

## Test Cases

- Session summary grouping: verifies log rows are grouped into sessions, ordered by recency, titled from action metadata, and counted correctly. Usefulness: 80/100.
- Session route helpers: verifies empty and `#/sessions` hashes route to the session list, session detail hashes decode IDs, and generated hashes encode session IDs safely. Usefulness: 70/100.
- Session list SQL: verifies the list query reads from `logs`, applies a recent timestamp cutoff, filters empty session IDs, orders by log time descending, includes body/time/message fields, and avoids `spans`/`trace_id`. Usefulness: 80/100.
- Session detail SQL: verifies the detail query reads from `logs`, escapes quoted session IDs, orders by log time ascending, and avoids `spans`/`trace_id`. Usefulness: 80/100.
