#!/bin/bash

set -e

RELEASE_VARIANT="--release"

if [ "$1" = "dev" ]; then
    RELEASE_VARIANT=""
fi

pushd crates
cargo build $RELEASE_VARIANT

pushd cojson-core-wasm
wasm-pack build --target nodejs $RELEASE_VARIANT # TODO: nodejs?  maybe bundler or web here?

popd
popd
