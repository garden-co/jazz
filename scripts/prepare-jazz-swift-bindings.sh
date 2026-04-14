#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$script_dir/generate-jazz-swift-bindings.sh"
bash "$script_dir/build-jazz-swift-host.sh"
bash "$script_dir/build-jazz-swift-ios.sh"
bash "$script_dir/build-jazz-swift-xcframework.sh"
