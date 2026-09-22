#!/usr/bin/env bash

# Validate the root schema.ts and permissions.ts.
node ./packages/jazz-tools/dist/cli.js build --schema-dir ./examples/todo-client-localfirst-ts
