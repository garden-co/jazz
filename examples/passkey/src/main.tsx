import { JazzProvider, PasskeyAuthBasicUI, usePasskeyAuth } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = usePasskeyAuth({
    appName: "Jazz Minimal Auth Passkey Example",
  });

  return (
    <>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=minimal-auth-passkey-example@garden.co"
      >
        {children}
      </JazzProvider>
      <PasskeyAuthBasicUI state={state} />
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzAndAuth>
      <App />
    </JazzAndAuth>
  </StrictMode>,
);
