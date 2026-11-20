---
"jazz-rn": patch
---

A React Native app pointed at a plaintext (`http`/`ws`) sync server other than this machine now fails with an error that says why and what to do, and whose `code` is `remote_plaintext_endpoint`. Plaintext stays limited to localhost, loopback addresses and the Android emulator hosts, so the session's token never travels unencrypted. The React Native setup docs describe how to develop on a physical device: `adb reverse` on Android, or a TLS tunnel.
