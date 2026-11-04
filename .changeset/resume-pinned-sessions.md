---
"jazz-tools": patch
"jazz-napi": patch
---

Clients now resume their subscriptions from where they last settled when they reconnect, instead of being sent every row again, including sessions pinned to a permissions bundle (every `createJazz` client) and sessions with claims. The server names each session's read context in its `Welcome`: a digest of the bundle it is pinned to and its claims. The client stamps each stored watermark with that digest, and the server resumes only when the reconnecting session has the same one. Under another bundle or other claims the read opens fresh, so rows the new permissions hide are evicted and rows they newly allow arrive. Claims that change with every token, such as `iat` or `exp`, make every reconnect open fresh. Watermarks a development store kept before this are dropped once at open. `Read` and `Welcome` change layout on the wire, so clients and servers from before and after this change can't decode each other's messages and must upgrade together.
