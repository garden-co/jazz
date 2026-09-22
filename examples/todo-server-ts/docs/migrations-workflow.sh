#!/usr/bin/env bash

# 1) Edit schema.ts.
# 2) Validate the current schema.
node ./packages/jazz-tools/dist/cli.js build --schema-dir ./examples/todo-server-ts
