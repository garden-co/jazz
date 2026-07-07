---
"jazz-tools": patch
---

Add an in-app inspector overlay to the Jazz dev plugins. During development the Vite, SvelteKit, and Next plugins now mount a floating toggle (or press `Alt+Shift+J`) that opens the embedded Jazz inspector docked at the bottom of your app — no separate window or setup. It's on by default whenever the dev plugin is in use and is dropped entirely from production builds. `jazz-inspector` now ships as a dependency of `jazz-tools`, so it no longer needs to be installed separately.
