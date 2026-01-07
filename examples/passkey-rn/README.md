# Jazz React Native Passkey Example

A minimal React Native app demonstrating Jazz passkey (WebAuthn) authentication using device biometrics (FaceID/TouchID/fingerprint).

## Features

- Passkey authentication with device biometrics
- Simple notes CRUD to demonstrate Jazz data sync
- Dark/light mode support

## Prerequisites

- Node.js 20+
- React Native development environment set up
- Physical iOS/Android device (passkeys don't work well in simulators)

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

## Development Notes

- Passkeys require HTTPS domain verification and won't work without proper configuration
- For development, consider using demo auth or passphrase auth instead
- Test on physical devices with biometric hardware

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
- [react-native-passkey](https://github.com/nicklockwood/react-native-passkey)
- [WebAuthn Spec](https://www.w3.org/TR/webauthn-3/)
