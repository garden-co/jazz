---
"jazz-tools": patch
---

`jazz-tools inspect login` now waits for the person to approve. It used to exit right after printing the approval URL, so the browser's hand-back to the terminal was refused and no token was stored.
