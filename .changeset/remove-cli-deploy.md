---
"jazz-tools": patch
---

Remove `jazz-tools deploy`. createJazz publishes your schema, its permissions and its migrations when your backend starts, so deploying the backend is the deployment. Running `jazz-tools deploy` now exits with an error that says so. `jazz-tools migrations create` and `migrations graph` are unchanged.
