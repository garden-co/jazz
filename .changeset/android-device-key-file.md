---
"jazz-tools": patch
---

Rust on Android: `DeviceKey::load_or_create(path)` creates the key. Until now it failed there with `Permission denied`, because Android denies apps the hard link it used. It now renames the key into place under a lock, so callers creating it at once still get one key. This leaves an empty `.<file name>.lock` beside the key. Other platforms are unchanged.
