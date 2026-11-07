---
"jazz-tools": patch
---

A local-first guest who signs up in one tab while another tab is open no longer has the tab they signed up in stop syncing. That tab's new client was refused by the shared worker, which the other tab still held as the guest. It ended `offline` (`stopped`), read nothing, and lost the writes it made on reload. Now it shows `connecting` and joins once the other tab follows the sign-up (or closes). Its queries, waits and writes made in the meantime go on from there. Any other configuration a worker refuses, such as an older deploy's schema in another tab, still fails visibly.
