---
"jazz-tools": patch
---

Store and sync every row version at one ordered position instead of a (node, seq) pair. A worker without a server now keeps its tabs' writes pending until a server is added, and a serializable transaction waits for that server instead of being validated by the worker. Local stores and the sync wire format from earlier builds of this alpha are not readable by this one: delete the local store and resync.
