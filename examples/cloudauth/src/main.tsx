import { JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { apiKey } from "./apiKey.ts";
import { CloudAuthBasicEmailUI } from "./components/CloudAuthBasicEmailUI.tsx";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  return (
    <JazzProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
        when: "signedUp",
      }}
    >
      <CloudAuthBasicEmailUI baseUrl="http://localhost:3000">
        {children}
      </CloudAuthBasicEmailUI>
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
