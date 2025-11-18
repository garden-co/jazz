// [!code hide]
const apiKey = "";
import { type SyncConfig } from "jazz-tools";

export const syncConfig: SyncConfig = {
  // Connection to Jazz Cloud or your own sync server
  peer: `wss://cloud.jazz.tools/?key=${apiKey}`,

  // When to sync: "always" (default), "never", or "signedUp"
  when: "always",
};
