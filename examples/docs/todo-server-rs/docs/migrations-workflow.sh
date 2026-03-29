#!/usr/bin/env bash

# #region migrations-workflow-rust
# 1) Edit schema.ts during development and watch for Jazz migration warnings.

# 2) Generate a typed migration stub after Jazz reports the old/new hashes.
npx jazz-tools migrations create <fromHash> <toHash>

# 3) Fill in migrate, rename the file, then publish the reviewed edge.
npx jazz-tools migrations push <fromHash> <toHash>
# #endregion migrations-workflow-rust
