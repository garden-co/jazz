---
"jazz-tools": patch
---

`vercel()` store: local-first guests (`initial: "device"`) now sync. The Sandbox core ran with local-first auth off, so it refused every guest (`offline`, reason `auth`, not retrying), and a guest's sign-up then failed to link with a `link_refused` that blamed the user for having another account. The core now admits guests, still only those your `authenticate` lets in with `acceptDeviceAccounts`. A core started by an older version is restarted by the first deployment of this one. If a core still refuses guests, the backend logs why (once), and linking fails with `local_first_auth_disabled` (retried later; the guest and its data stay on the device). A send to an HTTP stream session that has already ended is answered `410 stream session ended: <reason>`, no longer with a 404 suggesting sticky routing.
