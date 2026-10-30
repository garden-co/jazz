---
"jazz-tools": patch
---

Let Next.js apps import `jazz-tools/backend` statically under Turbopack when `jazz-napi` is a workspace link. Turbopack ignores `serverExternalPackages` for packages that resolve outside `node_modules`, so `withJazz` now aliases a workspace-linked `jazz-napi` to a module that loads it through Node at runtime.
