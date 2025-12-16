import { JazzExpoProvider } from "jazz-tools/expo";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";
import { RNCrypto } from "jazz-tools/react-native-core/crypto/RNCrypto";

export default function App() {
  return (
    <StrictMode>
      <JazzExpoProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        CryptoProvider={RNCrypto}
      >
        <ChatScreen />
      </JazzExpoProvider>
    </StrictMode>
  );
}
