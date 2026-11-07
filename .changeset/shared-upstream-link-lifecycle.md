---
"jazz-tools": patch
"@jazz/rust": patch
---

Every client now reconnects to its server the same way: React Native and the Rust client back off with jitter (100 ms up to 5 s) and report an outage after 7.5 s, as the web and Node runtimes already did. The Rust client no longer retries a refused token; it waits for a new one, which `JazzClient::update_auth_token` provides.
