---
"jazz-tools": minor
---

Allow serializable write waits to select a durability tier together with a timeout. An explicit Global wait stays pending offline, and a timeout also covers a commit whose transaction ID has not returned yet. Default tier selection and callback results are preserved.
