// #region e2ee-node-setup-ts
import { createJazzClient, type PrivateStore } from "jazz-tools/backend";
import { app } from "../jazz.generated.js";
import permissions from "../jazz.permissions.json" with { type: "json" };

export async function openNodeClient(
  serverUrl: string,
  dataPath: string,
  privateStore: PrivateStore,
) {
  return createJazzClient({
    serverUrl,
    app,
    permissions,
    driver: { type: "persistent", dataPath },
    initial: "device",
    encryption: { store: privateStore },
  });
}
// #endregion e2ee-node-setup-ts
