import { AuthKitProvider, useAuth } from '@workos-inc/authkit-react';
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzProviderWithWorkOS } from "jazz-react-auth-workos";
import { ReactNode } from "react";
import { apiKey } from "./apiKey";

// Import your publishable key
const CLIENT_ID = import.meta.env.VITE_WORKOS_CLIENT_ID;

if (!CLIENT_ID) {
  throw new Error("Add your WorkOS client id to the .env.local file");
}

function JazzProvider({ children }: { children: ReactNode }) {
  const workos = useAuth();
  
  return (
    <JazzProviderWithWorkOS
      workos={workos}
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      {children}
    </JazzProviderWithWorkOS>
  );
}

// Route to test that when the WorkO user expires, the app is logged out
if (location.search.includes("expirationTest")) {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <AuthKitProvider
        devMode
        clientId={CLIENT_ID}
      >
        <button onClick={() => {}}>Sign out</button>
      </AuthKitProvider>
    </StrictMode>,
  );
} else {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <AuthKitProvider
        devMode 
        clientId={CLIENT_ID}
      >
        <JazzProvider>
          <App />
        </JazzProvider>
      </AuthKitProvider>
    </StrictMode>,
  );
}
