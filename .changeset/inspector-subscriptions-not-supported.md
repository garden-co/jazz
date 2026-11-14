---
"jazz-tools": patch
---

The Inspector overlay's Subscriptions page no longer says "No active subscriptions" when it simply can't see them. A page's Db records subscriptions only from the moment the Inspector is mounted (or from the start with `devMode: true` in its client config), so the page now says which ones it lists, and that listing earlier ones is not supported yet. An empty result from the filters says so separately.
