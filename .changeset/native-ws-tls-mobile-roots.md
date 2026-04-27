---
"jazz-tools": patch
---

Fix native WebSocket TLS handshakes failing on mobile (Expo/React Native) when the OS root certificate store is empty or unavailable, by falling back to bundled `webpki` roots only when no native roots are loaded.
