import { LogBox } from "react-native";
import { JazzExpoProvider } from "jazz-tools/expo";
import { RNCrypto } from "jazz-tools/expo/crypto";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";

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

LogBox.ignoreLogs(["Open debugger to view warnings"]);
