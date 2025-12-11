import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
const apiKey = "you@example.com";
// #region RNC
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { RNCrypto } from "jazz-tools/react-native-core/crypto";

function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzReactNativeProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      CryptoProvider={RNCrypto}
    >
      {children}
    </JazzReactNativeProvider>
  );
}
// #endregion
