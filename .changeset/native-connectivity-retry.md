---
"jazz-napi": patch
"jazz-tools": patch
"jazz-rn": patch
"jazz-rn-ios": patch
"jazz-rn-android": patch
---

Keep native client relay ticks reconnectable after hostname-resolution failures or an idle TLS peer closes without `close_notify`. Unknown I/O, certificate, and protocol failures remain terminal.
