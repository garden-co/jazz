#!/usr/bin/env bash

# Regenerate SQL and typed TS bindings from schema/current.ts.
node ./packages/jazz-tools/dist/cli.js build --schema-dir ./examples/todo-client-localfirst-ts/schema
