---
"jazz-tools": patch
---

A background tab whose refused connection couldn't reach your backend for a moment (a network error, a restart) no longer signs out on the retry. The retry is asked as the refused check it replaces, so the tab still waits for focus, as it does when the first answer arrives.
