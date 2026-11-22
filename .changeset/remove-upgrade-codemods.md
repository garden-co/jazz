---
"jazz-tools": patch
---

**Breaking:** `jazz-tools upgrade` and its alpha.65 → alpha.66 codemods are removed. `npx jazz-tools upgrade` now exits 1 with a pointer to the upgrade guide (https://jazz.tools/docs/operating/backups-and-upgrades#upgrading-from-alpha70-to-beta0); an app still on alpha.65 or older can run `npx jazz-tools@2.0.0-alpha.70 upgrade` first, then follow the guide. The `jazz-tools validate` → "renamed to `generate` in alpha.65" message is gone too: `validate` is now an unknown command. Errors that pointed at `npx jazz-tools upgrade` (an app with `jazz.app.json` but no `schema.ts`, an alpha.64 `schema.ts`, a `permissions.ts` without a `permissions` export) now name `npx jazz-tools@2.0.0-alpha.70 upgrade` and the guide.
