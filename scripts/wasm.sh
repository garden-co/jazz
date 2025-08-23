#!/bin/bash

set -e

RELEASE_VARIANT="--release"

if [ "$1" = "dev" ]; then
    RELEASE_VARIANT=""
fi

pushd crates
# build all crates except cojson-core-rn (which will build as part of ios/android build)
cargo build $RELEASE_VARIANT --workspace --exclude cojson-core-rn

pushd cojson-core-wasm
wasm-pack build --target nodejs $RELEASE_VARIANT # TODO: nodejs?  maybe bundler or web here?

popd
popd
