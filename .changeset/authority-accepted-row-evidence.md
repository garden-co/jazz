---
"jazz-tools": minor
---

Add an authority-side engine read that pairs projected stored rows with the exact accepted origins of their state and retained content. Deletes and restores without replacement keep the content origin independently of their own state origin. Replica stores refuse this API rather than infer acceptance from incomplete local history.
