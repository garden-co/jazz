---
"jazz-tools": patch
---

Keep transport-state observers registered before lazy client creation attached
when the client becomes available. Preserve cancellation before and after binding
so closed observers cannot receive later connection or recovery notifications.
