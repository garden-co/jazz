---
"create-jazz": patch
---

Stream dependency installation output so verbose successful installs do not fail from exceeding Node's synchronous output buffer. Keep installer output hidden on success and include bounded stdout/stderr diagnostics on failure.
