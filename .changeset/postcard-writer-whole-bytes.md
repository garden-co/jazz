---
"jazz-tools": patch
---

Uploading a large value no longer blocks the JavaScript thread for seconds. Encoding each outgoing 1 MB piece took about 90 ms, so a 100 MB upload froze a Node backend's event loop (or a browser tab) for around 10 seconds. Large byte strings are now copied whole, which takes a few milliseconds.
