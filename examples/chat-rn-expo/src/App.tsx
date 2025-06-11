import { JazzProvider } from "jazz-expo";
import { Account } from "jazz-tools";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";

export default function App() {
  return (
    <StrictMode>
      <JazzProvider
        AccountSchema={Account}
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
      >
        <ChatScreen />
      </JazzProvider>
    </StrictMode>
  );
}
