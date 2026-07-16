import { createJazzContext } from "jazz-tools/backend";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import permissions from "../permissions.js";
import { app } from "../schema.js";

const jazzDbPath = process.env.JAZZ_DB ?? "./data/jazz.db";
mkdirSync(dirname(jazzDbPath), { recursive: true });

const context = createJazzContext({
  appId: process.env.JAZZ_APP_ID ?? "bluesky-offline-react-v2",
  app,
  permissions,
  driver: { type: "persistent", dataPath: jazzDbPath },
  serverUrl: process.env.JAZZ_SERVER_URL,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  env: "dev",
  userBranch: "main",
});

export const db = context.db();
