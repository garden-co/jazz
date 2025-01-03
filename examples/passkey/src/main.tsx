import { createJazzReactApp, usePasskeyAuth } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { PasskeyAuth } from "./PasskeyAuth.tsx";

const Jazz = createJazzReactApp();

export const { useAccount, useCoState } = Jazz;

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = usePasskeyAuth({
    appName: "Jazz Minimal Auth Passkey Example",
  });

  return (
    <>
      <Jazz.Provider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=minimal-auth-passkey-example@garden.co"
      >
        {children}
      </Jazz.Provider>
      <PasskeyAuth state={state} />
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
