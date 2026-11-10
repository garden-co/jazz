---
"jazz-tools": patch
---

A local-first guest now syncs its writes even when its browser also sends a signed-in user's cookie. That happens when the user signs up and the guest reconnects before it links to them. Before, `createJazz` refused the guest's socket in that case, so linking failed or the guest's writes were lost.
