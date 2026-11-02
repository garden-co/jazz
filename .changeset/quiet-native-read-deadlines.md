---
"jazz-napi": patch
"jazz-wasm": patch
---

Connect core read deadlines to the host scheduler so bounded reads complete on quiet connections, including when the scheduler is installed after a read starts.
