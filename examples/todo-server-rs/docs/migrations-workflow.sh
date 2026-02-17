#!/usr/bin/env bash

# 1) Edit schema/current.sql.
# 2) Generate migration stubs from the updated schema.
cargo run -p jazz-tools --features cli -- build --schema-dir ./examples/todo-server-rs/schema
