import { JazzProvider, betterAuth } from "jazz-tools/react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { SignInForm } from "./sign-in-form";
import "./App.css";
import { authClient } from "./auth-client";
const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string;

createRoot(document.getElementById("root")!).render(
  <JazzProvider
    appId={APP_ID}
    serverUrl={SERVER_URL}
    auth={betterAuth(authClient)}
    signedOut={
      <main className="page-center">
        <SignInForm />
      </main>
    }
  >
    <App />
  </JazzProvider>,
);
