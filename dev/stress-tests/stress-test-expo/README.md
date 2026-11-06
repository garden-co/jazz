# Stress Test Expo

Expo app for profiling and stress-testing Jazz on React Native.

## Profiling with Xcode Instruments

### Prerequisites

- Xcode installed with iOS platform support
- pnpm

### Steps

1. **Build all packages** from the repo root:

   ```bash
   pnpm build:all
   ```

2. **Install dependencies and prebuild** the Expo project:

   ```bash
   cd dev/stress-tests/stress-test-expo
   pnpm i
   pnpm expo prebuild --clean
   ```

3. **Install CocoaPods** to make sure native deps are linked correctly:

   ```bash
   cd ios
   pod install
   cd ..
   ```

4. **Start the app's backend** (createJazz on port 1625, serving `/jazz`; every device is a local-first guest of it), then the Metro bundler in another terminal:

   ```bash
   pnpm backend
   pnpm start
   ```

   Set `EXPO_PUBLIC_JAZZ_SERVER_URL` to the backend's `/jazz` URL when a device can't reach the defaults (`10.0.2.2:1625` on the Android emulator, `127.0.0.1:1625` on the iOS simulator).

5. **Open the project in Xcode:**

   Open `ios/stresstestexpo.xcworkspace` in Xcode.

6. **Run the profiler:**

   In Xcode, select **Product → Profile** (⌘I). Choose an Instruments template (e.g. Time Profiler, Allocations) and start recording.
