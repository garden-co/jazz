---
"jazz-tools": patch
---

jazz-ffi opens backend clients: a Go or Rust machine syncing through the app's main backend with its own token (`"auth": {"credential": true}`), trusted or scoped as that backend decides, with `for_user` on trusted clients (`jazz_for_user`) and closing that first drains queued writes (`shutdown_within`, `jazz_close_within`). New error kinds under contract 2: `noAccount` and `unsynced`.
