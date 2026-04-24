---
"jazz-tools": patch
---

Fix JazzProvider re-initialising when an inline config object is passed on every render. The useEffect dep array previously included `config` (the object reference); it now uses `configKey` (the JSON-stringified value) so that structurally identical config objects no longer trigger a cleanup→reacquire cycle.
