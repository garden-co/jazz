#!/bin/bash

# This script is necessary, because unlike ios, the android emulator action
# accepts a script, runs it as your tests, then terminates.

set -e

# build and install the app
cd ./android/
./gradlew installRelease
cd ..

# run the e2e tests
export PATH="$PATH":"$HOME/.maestro/bin"
export MAESTRO_DRIVER_STARTUP_TIMEOUT=300000 # setting to 5 mins 👀
export MAESTRO_CLI_NO_ANALYTICS=1
export MAESTRO_CLI_ANALYSIS_NOTIFICATION_DISABLED=true
maestro test test/e2e/flow.yml
