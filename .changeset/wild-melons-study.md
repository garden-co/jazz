---
"cojson-core-rn": minor
"jazz-tools": minor
---

## 💥 Breaking Changes

**Removed `RNQuickCrypto` in favor of `RNCrypto`**

`RNQuickCrypto` has been removed to streamline the library. `RNCrypto` is now the default implementation.

**Migration Guide:**

If you are currently using `RNCrypto`, you need to update your import paths:

**For React Native:**
```diff
- import { RNCrypto } from "jazz-tools/react-native-core/crypto/RNCrypto";
+ import { RNCrypto } from "jazz-tools/react-native-core/crypto";

```
**For Expo:**
```diff
-  import { RNCrypto } from "jazz-tools/react-native-core/crypto/RNCrypto";
+  import { RNCrypto } from "jazz-tools/expo/crypto";
```

If you are using `RNQuickCrypto`, you need to update as follow:

**For React Native:**
```diff
- import { RNQuickCrypto } from "jazz-tools/react-native-core/crypto";
+ import { RNCrypto } from "jazz-tools/react-native-core/crypto";

function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzReactNativeProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
-     CryptoProvider={RNQuickCrypto}
+     CryptoProvider={RNCrypto}
    >
      {children}
    </JazzReactNativeProvider>
  );
}
```
**For Expo:**
```diff
-  import { RNQuickCrypto } from "jazz-tools/expo/crypto";
+  import { RNCrypto } from "jazz-tools/expo/crypto";


function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzExpoProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=your-api-key" }}
-      CryptoProvider={RNQuickCrypto}
+      CryptoProvider={RNCrypto}
    >
      {children}
    </JazzExpoProvider>
  );
}
```


If you are not currently using `RNCrypto`, please refer to the [setup guide](https://jazz.tools/docs/react-native-expo/project-setup/providers#rncrypto).

