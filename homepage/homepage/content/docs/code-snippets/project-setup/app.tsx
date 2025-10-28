import { createRoot } from "react-dom/client";
import { JazzReactProvider } from "jazz-tools/react";
import { MyAppAccount } from "./schema";
// [!code hide]
const apiKey = "";

createRoot(document.getElementById("root")!).render(
  <JazzReactProvider
    sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
    AccountSchema={MyAppAccount}
  >
    {/* @ts-expect-error because App is not defined */}
    <App />
  </JazzReactProvider>
);