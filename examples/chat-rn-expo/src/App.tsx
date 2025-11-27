import { JazzExpoProvider } from "jazz-tools/expo";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";
import { RNQuickCrypto } from "jazz-tools/expo/crypto";

export default function App() {
  return (
    <StrictMode>
      <JazzExpoProvider
        CryptoProvider={RNQuickCrypto}
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
      >
        <ChatScreen />
      </JazzExpoProvider>
    </StrictMode>
  );
}
