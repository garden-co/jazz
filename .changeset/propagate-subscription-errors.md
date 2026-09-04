---
"jazz-tools": patch
---

Expose terminal errors through public `Db.subscribe` callback objects and propagate them through framework query entries. Subscription generations now fence buffered/admission/seed work after terminalization, legacy function callbacks report unhandled errors, and browser-worker relays preserve error names, messages, stacks, and serializable causes.
