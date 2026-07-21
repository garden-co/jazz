import { createJazzContext } from "jazz-tools/backend";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { jazzAppId } from "../shared/identifiers.js";
import { app } from "../schema.js";
import permissions from "../permissions.js";

const authenticationDbPath = "./data/auth.db";
const projectionDbPath = "./data/projection.db";
mkdirSync(dirname(authenticationDbPath), { recursive: true });

const serverUrl = process.env.JAZZ_SERVER_URL;
if (!serverUrl) throw new Error("JAZZ_SERVER_URL is required");

const backendSecret = process.env.BACKEND_SECRET;
if (!backendSecret) throw new Error("BACKEND_SECRET is required");

// Credentials remain durable in their own encrypted Jazz replica. Keeping
// them out of the projection replica means an obsolete projection outbox
// cannot prevent the BFF from restoring OAuth sessions.
const authenticationContext = createJazzContext({
  appId: jazzAppId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: authenticationDbPath },
  serverUrl,
  backendSecret,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  env: "dev",
  userBranch: "main",
});

// ATProto data uses its own replica, which can be reset independently of
// credentials if its local sync history becomes incompatible during development.
const projectionContext = createJazzContext({
  appId: jazzAppId,
  app,
  permissions,
  driver: { type: "persistent", dataPath: projectionDbPath },
  serverUrl,
  backendSecret,
  adminSecret: process.env.JAZZ_ADMIN_SECRET,
  env: "dev",
  userBranch: "main",
});

export const authenticationDb = authenticationContext.asBackend();
export const projectionDb = projectionContext.asBackend();
