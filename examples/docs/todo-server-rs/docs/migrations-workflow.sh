#!/usr/bin/env bash

# #region migrations-workflow-rust
# 1) Edit schema.ts, then generate jazz.app.json, jazz.permissions.json and
#    src/jazz_app.rs from it. generate refuses a changed schema until a
#    migration records the change.
npx jazz-tools generate

# 2) Write the migration stub, resolve any "TODO: …" it leaves, and generate
#    again: that writes the generated files and src/jazz_app.rs.
npx jazz-tools migrations create --name <name>
npx jazz-tools generate

# 3) Commit schema.ts, migrations/, jazz.app.json, jazz.permissions.json and
#    src/jazz_app.rs (CI runs
#    `npx jazz-tools generate --check`). Your TypeScript main backend
#    (createJazz) publishes the new version when it next starts, from the
#    same schema; this service opens the generated files as a backend client.
# #endregion migrations-workflow-rust
