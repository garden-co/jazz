import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
// #region QC
import { JazzExpoProvider } from "jazz-tools/expo";
import { RNQuickCrypto } from "jazz-tools/expo/crypto";

function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzExpoProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=your-api-key" }}
      CryptoProvider={RNQuickCrypto}
    >
      {children}
    </JazzExpoProvider>
  );
}
// #endregion
