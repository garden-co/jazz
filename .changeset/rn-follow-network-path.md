---
"jazz-rn": patch
---

On iOS, React Native's sync link now follows the device's network path: when Wi-Fi goes away the link is dropped and reconnected at once, and when the network comes back it reconnects without waiting out the retry backoff. A link on cellular is kept when Wi-Fi takes over. Android is unchanged. Thanks to Xelson for this.
