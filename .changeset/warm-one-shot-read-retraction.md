---
"jazz-tools": patch
---

Keep repeated one-shot reads of the same query fast now that subscriptions attach to already-live query shapes. A read that follows an unsubscribe of the same shape no longer runs an extra retraction pass before hydrating, so warm one-shot reads cost about the same as before that change. Results are unchanged.
