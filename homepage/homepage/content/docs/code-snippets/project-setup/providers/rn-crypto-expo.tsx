import React, { ReactNode } from "react";
function SignInScreen({ auth }: { auth: any }) {
  return null;
}
// #region RNC
import { JazzExpoProvider } from "jazz-tools/expo";
import { RNCrypto } from "jazz-tools/expo/crypto";

function MyJazzProvider({ children }: { children: ReactNode }) {
  return (
    <JazzExpoProvider
      sync={{ peer: "wss://cloud.jazz.tools/?key=your-api-key" }}
      CryptoProvider={RNCrypto}
    >
      {children}
    </JazzExpoProvider>
  );
}
// #endregion
