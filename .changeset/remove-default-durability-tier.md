---
"jazz-tools": patch
---

**Breaking:** Removed `defaultDurabilityTier`. Reads default to `"local-first"` in browsers and React Native, and without a configured server; other environments with a server default to `"remote"`. Set `tier` on individual reads to override this behavior.
