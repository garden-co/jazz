#!/usr/bin/env bash

# #region migrations-workflow-ts
# 1) Edit schema/current.ts.
# 2) Generate migration stubs from the updated schema.
pnpm --filter todo-server-ts exec jazz-ts build --schema-dir ./schema
# #endregion migrations-workflow-ts
