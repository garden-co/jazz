---
"jazz-tools": patch
---

Fix `jazzPlugin()` (`jazz-tools/dev/vite`) so its return type matches Vite's `Plugin`. The `config` hook's parameter previously typed `ssr.external` as `string[] | undefined`, but Vite's `UserConfig` allows `true | string[] | undefined` (`true` = externalize everything), causing `TS2769` in consumer `vite.config.ts` files. Widen the param and preserve `external: true` when the user already opts into it.
