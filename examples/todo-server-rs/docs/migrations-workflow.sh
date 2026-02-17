#!/usr/bin/env bash

# #region migrations-workflow-sql
# 1) Edit schema/current.sql.
# 2) Generate migration stubs from the updated schema.
cargo run -p jazz-cli -- build --schema-dir ./examples/todo-server-rs/schema
# #endregion migrations-workflow-sql
