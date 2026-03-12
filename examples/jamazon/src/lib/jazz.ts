import { APP_ID, JAZZ_SERVER_PORT } from "@/config";
import { createJazzClient } from "jazz-tools/react";

export const jazzClient = createJazzClient({
  appId: APP_ID,
  serverUrl: `http://localhost:${JAZZ_SERVER_PORT}`,
  env: "dev",
  userBranch: "main",
  localAuthMode: "anonymous",
});
