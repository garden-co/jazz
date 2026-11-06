---
"jazz-tools": patch
---

Large local-first uploads over the HTTP stream (a `createJazz` backend whose host can't upgrade sockets, such as `next start`) now complete. A session that received more than its 8 MiB ingress budget at once was dropped, and the upload started over on every reconnect, so a 100 MB file never arrived. The core now stops reading a session until its engine has taken earlier messages, and the backend passes that pace back to the client: an HTTP stream send is answered once the core has room for it, a socket is paused meanwhile (custom hosts handing sockets to `acceptSocket` can implement the optional `pause`/`resume`), and each HTTP stream send carries at most 4 MB. A client that keeps sending far past what the server takes, over a transport that can't be paused, still has its session ended.
