#!/bin/bash

# This script is necessary, because unlike ios, the android emulator action
# accepts a script, runs it as your tests, then terminates.

set -e

# build and install the app
echo "Building and installing Android app."
echo "If it fails, its output will be in artifact: android-install.log..."
cd ./android/
./gradlew installRelease >> ~/output/android-install.log 2>&1
cd ..

# run the e2e tests
export PATH="$PATH":"$HOME/.maestro/bin"
export MAESTRO_DRIVER_STARTUP_TIMEOUT=300000 # setting to 5 mins 👀
export MAESTRO_CLI_NO_ANALYTICS=1
export MAESTRO_CLI_ANALYSIS_NOTIFICATION_DISABLED=true
maestro test test/e2e/flow.yml
