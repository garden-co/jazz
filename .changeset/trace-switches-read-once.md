---
"jazz-tools": patch
---

Debug trace environment variables (`JAZZ_COVERED_INPUT_TRACE`, `JAZZ_REHYDRATE_TRACE`, `JAZZ_QUERY_TEMPLATE_TRACE` and `JAZZ_FORCE_SINGLETON_VERSION_CARRIERS`) are now read once per process instead of on every send and delta, so set them before the process starts. Refreshes also match changed storage tables to schema tables without formatting every candidate name.
