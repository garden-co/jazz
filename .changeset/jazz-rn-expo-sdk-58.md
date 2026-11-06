---
"jazz-rn": patch
---

Install on Expo SDK 55+ (React Native 0.82+), including today's `create-expo-app` default (SDK 58): `pod install` no longer rejects the app as legacy-architecture, and the config plugin no longer adds a `newArchEnabled` field that fails `expo-doctor`. The iOS relay is now built for iOS 15.1, so apps targeting older iOS than the build machine's SDK no longer link objects marked for a newer iOS. Release-candidate tarballs can find their native payload again.
