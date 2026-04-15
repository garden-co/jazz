#!/usr/bin/env bash

# 1) Edit schema.ts during development and watch for Jazz migration warnings.

# 2) Generate a typed migration stub after Jazz reports the old/new hashes.
npx jazz-tools@alpha migrations create --fromHash <fromHash> --toHash <toHash>

# 3) Fill in migrate, rename the file, then publish the reviewed edge.
npx jazz-tools@alpha migrations push <fromHash> <toHash>
