import { createRoot } from "react-dom/client";
import { JazzReactProvider } from "jazz-tools/react";
import { MyAppAccount } from "./schema";
// [!code hide:2]
const apiKey = "";
const App = () => null;

createRoot(document.getElementById("root")!).render(
  <JazzReactProvider
    sync={{ peer: `wss://cloud.jazz.tools/?key=${apiKey}` }}
    AccountSchema={MyAppAccount}
  >
    <App />
  </JazzReactProvider>,
);
