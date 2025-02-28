import { ClerkProvider } from "@clerk/clerk-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { MyAccount } from "./schema.ts";
import "./index.css";
import { apiKey } from "./apiKey.ts";
import { OmniAuth } from "./components/OmniAuth.tsx";

declare module "jazz-react" {
  export interface Register {
    Account: MyAccount;
  }
}

// Import your publishable key
const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

if (!PUBLISHABLE_KEY) {
  throw new Error("Add your Clerk publishable key to the .env.local file");
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
      <OmniAuth
        AccountSchema={MyAccount}
        sync={{
          peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
          when: "signedUp", // This makes the app work in local mode when the user is not authenticated
        }}
      >
        <App />
      </OmniAuth>
    </ClerkProvider>
  </StrictMode>,
);
