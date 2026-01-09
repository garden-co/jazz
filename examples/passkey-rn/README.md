# Jazz React Native Passkey Example

A minimal React Native app demonstrating Jazz passkey (WebAuthn) authentication using device biometrics (FaceID/TouchID/fingerprint).

## Features

- Passkey authentication with device biometrics
- Simple notes CRUD to demonstrate Jazz data sync
- Dark/light mode support

## Prerequisites

- Node.js 20+
- React Native development environment set up
- Physical iOS/Android device (passkeys don't work in simulators)
- A domain with HTTPS for hosting AASA/assetlinks files

## Setup

1. Install dependencies from the monorepo root:

```bash
cd /path/to/jazz
pnpm install
```

2. Install iOS pods:

```bash
cd examples/passkey-rn
pnpm pods
```

## Running the App

### iOS

```bash
pnpm ios
```

### Android

```bash
pnpm android
```

## Passkey Configuration

**Important**: Passkeys require domain verification to work. Before testing passkeys on a real device, you must:

### 1. Update the Domain

In `src/AuthScreen.tsx`, change `rpId` to your domain:

```typescript
const auth = usePasskeyAuth({
  appName: "My App",
  rpId: "yourdomain.com", // Your actual domain
});
```

### 2. iOS Setup

1. Add Associated Domains capability in Xcode:
   - Open `ios/PasskeyRN.xcodeproj`
   - Select the target → Signing & Capabilities
   - Add "Associated Domains" capability
   - Add: `webcredentials:yourdomain.com`

2. Host an Apple App Site Association file at:
   `https://yourdomain.com/.well-known/apple-app-site-association`

```json
{
  "webcredentials": {
    "apps": ["TEAM_ID.com.passkeyrn"]
  }
}
```

### 3. Android Setup

1. Host an assetlinks.json file at:
   `https://yourdomain.com/.well-known/assetlinks.json`

```json
[{
  "relation": ["delegate_permission/common.get_login_creds"],
  "target": {
    "namespace": "android_app",
    "package_name": "com.passkeyrn",
    "sha256_cert_fingerprints": ["YOUR_CERT_FINGERPRINT"]
  }
}]
```

## Troubleshooting

### New Architecture / Bridgeless Mode

The `react-native-passkey` library uses the legacy `NativeModules` bridge pattern and doesn't yet support TurboModules. In React Native 0.76+ with New Architecture enabled (which is the default), you need to disable bridgeless mode.

Add this override to your `AppDelegate.swift`:

```swift
class ReactNativeDelegate: RCTDefaultReactNativeFactoryDelegate {
  // ... existing methods ...

  // Disable bridgeless mode to support legacy NativeModules (react-native-passkey)
  override func bridgelessEnabled() -> Bool {
    return false
  }
}
```

This keeps the New Architecture (Fabric, TurboModules) enabled while also maintaining the legacy bridge for backward compatibility with libraries like react-native-passkey.

### Monorepo / Metro Resolution Issues

If you're using a monorepo and encounter "Unknown named module" errors when loading react-native-passkey, you can inject the module manually in your entry file:

```javascript
// index.js
import "jazz-tools/react-native/polyfills";
import { Passkey } from "react-native-passkey";
import { setPasskeyModule } from "jazz-tools/react-native-core";

// Inject the passkey module to avoid dynamic require issues
setPasskeyModule(Passkey);

// ... rest of your app
```

You may also need to add a custom resolver to your `metro.config.js`:

```javascript
resolver: {
  resolveRequest: (context, moduleName, platform) => {
    if (moduleName === "react-native-passkey") {
      return {
        type: "sourceFile",
        filePath: path.resolve(workspaceRoot, "node_modules/react-native-passkey/lib/module/index.js"),
      };
    }
    return context.resolveRequest(context, moduleName, platform);
  },
}
```

### "The package doesn't seem to be linked" Error

If you see this error at runtime, check that:
1. You've run `pod install` after adding react-native-passkey
2. You've rebuilt the app (not just reloaded JS)
3. Bridgeless mode is disabled (see above)

### Android Build Hangs at configureCMakeDebug

If your Android build hangs indefinitely at `:app:configureCMakeDebug[arm64-v8a]` (specifically at `_CMakeLTOTest-CXX`), upgrade your NDK version. NDK 27.x has issues with the CMake LTO test when cross-compiling.

In `android/build.gradle`, use NDK 28.2 or later:

```groovy
buildscript {
    ext {
        ndkVersion = "28.2.13676358"
    }
}
```

Then clean and rebuild:

```bash
cd android && rm -rf app/.cxx .gradle && cd ..
pnpm android
```

### Android App Crashes on Startup (SoLoader)

If the app crashes with `SoLoader.init() not yet called` or `Feature flags cannot be overridden more than once`, ensure your `MainApplication.kt` only calls `loadReactNative()`:

```kotlin
override fun onCreate() {
    super.onCreate()
    loadReactNative(this)
}
```

Do **not** separately call `DefaultNewArchitectureEntryPoint.load()` - this is already handled by `loadReactNative()`.

## Development Notes

- Passkeys require HTTPS domain verification and won't work without proper configuration
- For development, consider using demo auth or passphrase auth instead
- Test on physical devices with biometric hardware
- The library's own example app doesn't enable New Architecture, which is why there are no open issues about this

## Project Structure

```
src/
├── App.tsx           # Main app with Jazz provider
├── AuthScreen.tsx    # Passkey authentication UI
├── NotesScreen.tsx   # Simple notes demo
├── schema.ts         # Jazz data schema
└── apiKey.ts         # Jazz API key
```

## Related

- [Jazz Documentation](https://jazz.tools/docs)
- [react-native-passkey](https://github.com/f-23/react-native-passkey)
- [WebAuthn Spec](https://www.w3.org/TR/webauthn-3/)
