---
"jazz-tools": patch
---

Add React Native passkey (WebAuthn) authentication support

New exports from `jazz-tools/react-native-core`:
- `ReactNativePasskeyAuth`: Core auth class for passkey authentication
- `usePasskeyAuth`: React hook for passkey auth state management
- `PasskeyAuthBasicUI`: Ready-to-use auth UI component with dark/light mode support
- `isPasskeySupported`: Helper to check device passkey support

Uses `react-native-passkey` as an optional peer dependency. Requires domain configuration (AASA for iOS, assetlinks.json for Android) for passkey verification.
