import { ClerkProvider, SignInButton, useClerk } from "@clerk/clerk-react";
import { useJazzClerkAuth } from "jazz-react-auth-clerk";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzProvider } from "jazz-react";

// Import your publishable key
const PUBLISHABLE_KEY = import.meta.env.VITE_CLERK_PUBLISHABLE_KEY;

if (!PUBLISHABLE_KEY) {
  throw new Error("Add your Clerk publishable key to the .env.local file");
}

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const clerk = useClerk();
  const [auth, state] = useJazzClerkAuth(clerk);

  return (
    <main className="container">
      {state?.errors?.map((error) => (
        <div key={error}>{error}</div>
      ))}
      {clerk.user && auth ? (
        <JazzProvider
          auth={auth}
          peer="wss://cloud.jazz.tools/?key=minimal-auth-clerk-example@garden.co"
        >
          {children}
        </JazzProvider>
      ) : (
        <SignInButton />
      )}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ClerkProvider publishableKey={PUBLISHABLE_KEY} afterSignOutUrl="/">
      <JazzAndAuth>
        <App />
      </JazzAndAuth>
    </ClerkProvider>
  </StrictMode>,
);
