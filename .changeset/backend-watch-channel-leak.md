---
"jazz-tools": patch
---

A browser `createJazzClient` whose startup fails (the backend unreachable while it opens, a refused configuration, a local-first client that doesn't load) no longer leaves its cross-tab `BroadcastChannel` open. The session's channel to other tabs now opens only while its backend watch listens, so a provider that retries a failed start doesn't leak one channel, with its listener, per attempt.
