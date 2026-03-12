import { ADMIN_SECRET, APP_ID, JAZZ_SERVER_PORT } from "@/config";
import { createDb } from "jazz-tools/backend";
import type { LocalAuthMode } from "jazz-tools/backend";

export function getServerDb(
  localAuthToken = "jamazon-admin",
  localAuthMode: LocalAuthMode = "anonymous",
) {
  const token = localAuthToken.trim() || "jamazon-admin";
  return createDb({
    appId: APP_ID,
    serverUrl: `http://localhost:${JAZZ_SERVER_PORT}`,
    env: "dev",
    userBranch: "main",
    localAuthMode,
    localAuthToken: token,
    adminSecret: ADMIN_SECRET,
  });
}
