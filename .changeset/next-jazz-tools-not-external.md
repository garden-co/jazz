---
"jazz-tools": patch
---

`withJazz` no longer adds `jazz-tools` to Next.js `serverExternalPackages`. Externalising it caused the SSR worker and the user's "use client" components to load separate React instances, so the SSR dispatcher was missing on jazz-tools' copy and `useSyncExternalStore` failed with `Cannot read properties of null` when prerendering pages like `/_not-found`. `jazz-napi` stays external (native binary); jazz-tools is now bundled.
