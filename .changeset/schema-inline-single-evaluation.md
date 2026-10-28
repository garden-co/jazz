---
"jazz-tools": patch
---

Derive schema and inline permissions from one module evaluation. Without a separate permissions file, schema module side effects now run once per load, while independent loads remain isolated.
