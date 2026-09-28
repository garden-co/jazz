---
"jazz-napi": patch
"jazz-tools": patch
"jazz-rn": patch
"jazz-rn-ios": patch
"jazz-rn-android": patch
---

Keep native client relay ticks recoverable after temporary socket closure and HTTP connection failures (408, 425, 429, 500, 502, 503, 504), as well as hostname-resolution failures or TLS EOF without `close_notify`. Authentication, malformed protocol, certificate, and unclassified I/O failures remain terminal.
