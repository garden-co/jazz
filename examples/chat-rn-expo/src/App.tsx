import { JazzExpoProvider } from "jazz-tools/expo";
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
      >
        <ChatScreen />
      </JazzExpoProvider>
    </StrictMode>
  );
}
