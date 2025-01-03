import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { JazzAccount } from "./schema.ts";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, authState] = useDemoAuth();

  return (
    <>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=image-upload-example@garden.co"
        AccountSchema={JazzAccount}
      >
        {children}
      </JazzProvider>

      {authState.state !== "signedIn" && (
        <DemoAuthBasicUI appName="Image upload" state={authState} />
      )}
    </>
  );
}

declare module "jazz-react" {
  interface Register {
    Account: JazzAccount;
  }
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </StrictMode>,
);
