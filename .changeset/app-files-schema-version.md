---
"jazz-tools": patch
---

**`jazz.app.json` and `jazz.permissions.json` are versioned by `$schema`; `format` is gone.** Each file now starts with a required `$schema` that names its JSON Schema and so its version: `https://jazz.tools/schemas/jazz.app.v1.json` and `https://jazz.tools/schemas/jazz.permissions.v1.json`. Jazz refuses a file written by a newer version and asks you to upgrade. A file without `$schema` is refused with a hint to run `jazz-tools generate`. Files written before alpha.66 carry `"format": 1` and an unversioned `$schema`; they still load as version 1, and `npx jazz-tools generate` rewrites them in the new form (`generate --check` only warns about them). `jazz-tools upgrade` writes the new form, and `createJazz` takes both forms. The old JSON Schema URLs keep working in editors.
