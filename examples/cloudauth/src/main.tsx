import { JazzProvider } from "jazz-react";
import { CloudAuthBasicUI } from "jazz-react-auth-cloudauth";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { apiKey } from "./apiKey.ts";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        when: "signedUp",
      }}
    >
      <CloudAuthBasicUI appName="Jazz Minimal Auth Passkey Example">
        {children}
      </CloudAuthBasicUI>
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </StrictMode>,
);
