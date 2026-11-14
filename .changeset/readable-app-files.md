---
"jazz-tools": patch
---

`jazz-tools validate` and `migrations create` now also write two readable JSON files beside `schema.ts`: `jazz.app.json` (your tables, their relations and the schema history your app ships) and `jazz.permissions.json` (your current permissions, read only by backends). Nothing else changes: your app keeps running from `schema.ts`, `permissions.ts` and `migrations/index.ts`. Commit both files; other languages, such as Rust's `jazz::tools::deployment::artifact::load`, read them. Keep formatters off them.

`validate --check` also fails when either file is missing or out of date. `createJazz` refuses to start when the committed files no longer match your TypeScript, naming the file to regenerate with `npx jazz-tools validate`. A project without the files starts as before, and an app the files can't describe yet gets a warning. Policy expressions in the files read like SQL; see https://jazz.tools/docs/reference/app-files.
