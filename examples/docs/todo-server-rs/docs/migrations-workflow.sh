#!/usr/bin/env bash

# #region migrations-workflow-rust
# 1) Edit schema.ts and run the validator.
npx jazz-tools@alpha build

# 2) Generate a typed migration stub after Jazz reports the old/new hashes.
npx jazz-tools@alpha migrations create <fromHash> <toHash>

# 3) Fill in migrate(), rename the file, then publish the reviewed edge.
npx jazz-tools@alpha migrations push <fromHash> <toHash>
# #endregion migrations-workflow-rust
