---
"jazz-tools": patch
---

Signing out, or being signed out by the backend, no longer fails when the server won't take this device's remaining writes. Before, a user whose backend session ended (cookies cleared, expired) with a write still on the device was stuck on "Graceful shutdown could not synchronize pending writes; the context remains open", through Retry, sign-out and reloads, with no way back to sign-in. Jazz now uploads what the server still accepts, warns about the rest, and signs out. A persistent client keeps those writes on this device and uploads them when that account signs in again.
