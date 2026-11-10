---
"jazz-tools": minor
---

Breaking: the framework providers no longer try to open the Inspector overlay in development, and their `autoAttachDevTools` prop is gone. That auto-attach only ran when the removed Jazz bundler plugins had set `VITE_JAZZ_INSPECTOR` or `NEXT_PUBLIC_JAZZ_INSPECTOR`, so since alpha.60 it never did anything. Providers that would otherwise pass an unknown prop on to the client config (React's and Solid's `JazzProvider` and `JazzSessionProvider`, Solid's `JazzClientProvider`, Svelte's `JazzProvider` and `JazzSvelteProvider`, and Vue's `JazzProvider`, `JazzClientProvider` and `JazzSessionProvider`) now refuse `autoAttachDevTools` with an error: remove it. To show the overlay, call `mountInspector(db)` from `jazz-tools/inspector` in development. A provider given a `config` also stops turning on `devMode` by itself; set `devMode: true` in the config if you want the overlay's Subscriptions tab to see subscriptions opened at startup.
