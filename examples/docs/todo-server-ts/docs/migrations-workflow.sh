#!/usr/bin/env bash

# #region migrations-workflow-ts
# 1) Edit schema/current.ts.
# 2) Generate migration stubs from the updated schema.
npx jazz-tools@alpha build
# #endregion migrations-workflow-ts
