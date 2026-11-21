---
"jazz-tools": patch
---

**Breaking:** the schema-summary endpoint `GET /apps/<appId>/schemas` no longer returns the `hashes` field, deprecated since alpha.54. Read the hashes from `schemas` instead (`schemas[].hash`, next to `schemas[].publishedAt`). `fetchSchemaHashes` still returns `hashes`, now taken from `schemas`. Scripts that call the endpoint directly and read `hashes` need to switch to `schemas`.
