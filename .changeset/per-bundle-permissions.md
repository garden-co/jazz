---
"jazz-tools": patch
"jazz-napi": patch
---

`createJazz` no longer makes its permissions current on the core when it boots. It publishes them as a bundle, addressed by its content, and the core governs every session that backend opens or relays (browser sockets, guests, and its own `forReqUser` writes) by that bundle alone. Two deployments of an app on one core now each serve their sessions by their own permissions, so a newer instance booting no longer takes over an older one's sessions, and a rollback is publishing the older bundle again. Upgrade the sync server before the app: an older server publishes no bundle, and `createJazz` refuses to start against it.
