import "./app.css";
import { StrictMode } from "react";
import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";
import { createRoot } from "react-dom/client";
import { Toaster } from "@/components/ui/sonner.tsx";
import { getRandomUsername, inIframe } from "@/lib/utils";
import { ChatAccount } from "@/schema.ts";
import App from "./App.tsx";
import { apiKey } from "./apiKey.ts";

const url = new URL(window.location.href);
const defaultProfileName = url.searchParams.get("user") ?? getRandomUsername();

// biome-ignore lint/style/noNonNullAssertion: We know root exists.
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzReactProvider
      authSecretStorageKey="examples/chat-react"
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      defaultProfileName={defaultProfileName}
      AccountSchema={ChatAccount}
    >
      <App />
      {!inIframe && <JazzInspector />}
      <Toaster />
    </JazzReactProvider>
  </StrictMode>,
);
