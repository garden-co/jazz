import { JazzProvider } from "jazz-expo";
import React, { StrictMode } from "react";
import { apiKey } from "./apiKey";
import ChatScreen from "./chat";
import { ChatAccount } from "./schema";

export default function App() {
  return (
    <StrictMode>
      <JazzProvider
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        }}
        AccountSchema={ChatAccount}
      >
        <ChatScreen />
      </JazzProvider>
    </StrictMode>
  );
}
