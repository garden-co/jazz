#!/usr/bin/env bash

# #region schemas-ts-codegen-cli
# Regenerate SQL and typed TS bindings from schema/current.ts.
pnpm --filter todo-client-localfirst-ts exec jazz-ts build
# #endregion schemas-ts-codegen-cli
