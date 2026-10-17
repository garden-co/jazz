---
"create-jazz": patch
---

Report starter delete progress through local persistence, preserve local delete failures during concurrent writes, and surface later authority rejections for all mutations without retaining completed delete handles or waiting indefinitely for a disconnected server.
