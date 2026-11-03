---
"jazz-tools": patch
"jazz-napi": patch
---

A blob dir is claimed exclusively by one data dir (also on file systems without hard links), blob dir paths resolve symlinks, and `jazz-tools server` locks its data dir with the same lock embedded cores take. A store whose blob dir was recorded through a symlinked path on a pre-release build (for example macOS `/tmp`, which is `/private/tmp`) may need its `blob-store` marker reset.
