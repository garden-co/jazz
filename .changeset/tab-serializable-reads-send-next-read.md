---
"jazz-tools": patch
---

Fix a browser tab's serializable transaction hanging on its second read when several reads follow each other behind the shared worker: the tab now asks for the tick that sends each next read.
