import { DemoAuthBasicUI, JazzProvider, useDemoAuth } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

import "./index.css";

function JazzAndAuth({ children }: { children: React.ReactNode }) {
  const [auth, state] = useDemoAuth();

  return (
    <>
      <JazzProvider
        auth={auth}
        peer="wss://cloud.jazz.tools/?key=richtext-example@garden.co"
      >
        {children}
      </JazzProvider>
      <DemoAuthBasicUI state={state} appName="Jazz Rich Text Example" />
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
