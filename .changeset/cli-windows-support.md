---
"jazz-tools": patch
---

Stop advertising a Windows `jazz-tools` CLI binary that the release workflow does not produce. Windows users now get a clear unsupported-platform error instead of a misleading missing-artifact error; Windows NAPI builds remain supported.
