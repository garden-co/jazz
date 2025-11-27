import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
const apiKey = "you@example.com";
// #region QC
import { JazzReactNativeProvider } from "jazz-tools/react-native";
import { RNQuickCrypto } from "jazz-tools/react-native/crypto";

function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzReactNativeProvider
      sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
      CryptoProvider={RNQuickCrypto}
    >
      {children}
    </JazzReactNativeProvider>
  );
}
// #endregion
