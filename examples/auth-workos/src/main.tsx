import { AuthKitProvider } from "@workos-inc/authkit-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

// WorkOS Client ID for this demo
const WORKOS_CLIENT_ID = "client_01JX28XKCGFWXHBMX2FW66JTRM";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <AuthKitProvider clientId={WORKOS_CLIENT_ID}>
      <App />
    </AuthKitProvider>
  </StrictMode>,
);
