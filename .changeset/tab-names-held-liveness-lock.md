---
"jazz-tools": patch
---

Fix a browser tab that could rarely stop hearing from its shared worker right after it connected, which left its queries and waits hanging. The worker watches a lock each tab holds for as long as its page lives, so it notices a tab that went away. It could ask for that lock before the tab held it, get it at once, and drop the live tab. A tab now names its lock to the worker only once it holds it.

Also, a backend watch that keeps getting 404s now stops after exactly its third answer. It had compared the clock with its retry waits, and a 1 ms disagreement between them could add one more retry.
