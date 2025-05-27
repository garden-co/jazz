import { JazzProvider } from "jazz-expo";
import { RNQuickCrypto } from "jazz-expo/crypto";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";

export default function App() {
  return (
    <StrictMode>
      <JazzProvider
        CryptoProvider={RNQuickCrypto}
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
      >
        <ChatScreen />
      </JazzProvider>
    </StrictMode>
  );
}
