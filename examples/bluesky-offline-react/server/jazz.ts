import { createJazzContext } from "jazz-tools/backend";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { appId } from "../app-id.js";
import permissions from "../permissions.js";
import { app } from "../schema.js";

const jazzDbPath = "./data/jazz.db";
mkdirSync(dirname(jazzDbPath), { recursive: true });

const serverUrl = process.env.JAZZ_SERVER_URL;
if (!serverUrl) throw new Error("JAZZ_SERVER_URL is required");

const backendSecret = process.env.BACKEND_SECRET;
if (!backendSecret) throw new Error("BACKEND_SECRET is required");

const context = createJazzContext({
  appId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: jazzDbPath },
  serverUrl,
  backendSecret,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  env: "dev",
  userBranch: "main",
});

export const db = context.asBackend();
