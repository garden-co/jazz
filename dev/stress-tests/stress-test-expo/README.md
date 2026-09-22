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
   cd examples/stress-test-expo
   pnpm i
   pnpm expo prebuild --clean
   ```

3. **Install CocoaPods** to make sure native deps are linked correctly:

   ```bash
   cd ios
   pod install
   cd ..
   ```

4. **Start the Metro bundler:**

   ```bash
   pnpm start
   ```

5. **Open the project in Xcode:**

   Open `ios/stresstestexpo.xcworkspace` in Xcode.

6. **Run the profiler:**

   In Xcode, select **Product → Profile** (⌘I). Choose an Instruments template (e.g. Time Profiler, Allocations) and start recording.
