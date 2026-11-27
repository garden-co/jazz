// #region e2ee-browser-setup-ts
import { createJazzClient } from "jazz-tools/client";
import { app } from "../jazz.generated.js";

export async function openBrowserClient() {
  return createJazzClient({
    serverUrl: "/jazz",
    initial: "device",
    encryption: { app },
    client: () => import("jazz-tools/local-first"),
  });
}
// #endregion e2ee-browser-setup-ts
