#!/bin/bash

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

rm -rf ./crates/cojson-core-rn/pkg/build
rm -rf ./crates/cojson-core-rn/pkg/dist
rm -rf ./examples/*/node_modules
rm -rf ./examples/*/dist
rm -rf ./packages/*/dist
rm -rf ./packages/*/node_modules

if [ "$1" = "deep" ]; then
    rm -rf ./node_modules
    rm -rf ./crates/target
fi
