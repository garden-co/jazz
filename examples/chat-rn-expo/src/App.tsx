import { JazzExpoProvider } from "jazz-tools/expo";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";
import { RNQuickCrypto } from "jazz-tools/expo/crypto";

export default function App() {
  return (
    <StrictMode>
      <JazzExpoProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        CryptoProvider={RNQuickCrypto}
      >
        <ChatScreen />
      </JazzExpoProvider>
    </StrictMode>
  );
}
