#!/usr/bin/env bash

# #region migrations-workflow-sql
# 1) Edit schema/current.sql.
# 2) Generate migration stubs from the updated schema.
npx jazz-tools@alpha build
# #endregion migrations-workflow-sql
