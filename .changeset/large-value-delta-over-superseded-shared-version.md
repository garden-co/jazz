---
"jazz-tools": patch
---

Fix an edit to a large value (`s.bytes()`, long text) being refused with `WriteRejected` ("names no version of its row the writer may read") when the writer was granted access after the row's last write and another write landed first. The edit now lands over the newer version like any concurrent edit. A version counts as readable when the writer could read it at some point while it was current, judged by the server's own history. Versions superseded before the grant stay unreadable.
