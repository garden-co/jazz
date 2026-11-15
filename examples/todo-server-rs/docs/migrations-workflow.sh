#!/usr/bin/env bash

# 1) Edit the tables in jazz.app.json, then check the files and regenerate
#    the bindings. generate refuses a changed schema until a migration
#    records the change.
npx jazz-tools generate

# 2) Write the migration stub, resolve any "TODO: …" it leaves, and generate
#    again: that also regenerates src/jazz_app.rs.
npx jazz-tools migrations create --name <name>
npx jazz-tools generate

# 3) Commit jazz.app.json and src/jazz_app.rs (CI runs
#    `npx jazz-tools generate --check`). The backend publishes the new
#    version with its permissions bundle when it next starts.
