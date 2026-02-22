#!/usr/bin/env bash

# 1) Edit schema/current.ts.
# 2) Generate migration stubs from the updated schema.
node ./packages/jazz-tools/dist/cli.js build --schema-dir ./examples/todo-server-ts/schema
