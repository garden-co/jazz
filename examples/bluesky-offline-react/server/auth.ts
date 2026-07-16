import {
  buildAtprotoLoopbackClientMetadata,
  NodeOAuthClient,
  type NodeSavedSession,
  type NodeSavedState,
} from "@atproto/oauth-client-node";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const port = Number(process.env.PORT ?? 3001);
export const oauthScope = "atproto transition:generic";
const clientMetadata = buildAtprotoLoopbackClientMetadata({
  redirect_uris: [`http://127.0.0.1:${port}/api/auth/callback`],
  scope: oauthScope,
});

const oauthSessionsPath = process.env.OAUTH_SESSIONS_FILE ?? "./data/oauth-sessions.json";
mkdirSync(dirname(oauthSessionsPath), { recursive: true });

function loadPersistentMap<T>(path: string) {
  try {
    return new Map(Object.entries(JSON.parse(readFileSync(path, "utf8"))) as [string, T][]);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn(`Could not load OAuth sessions from ${path}:`, error);
    }
    return new Map<string, T>();
  }
}

function savePersistentMap<T>(path: string, values: Map<string, T>) {
  const temporaryPath = `${path}.tmp`;
  writeFileSync(temporaryPath, JSON.stringify(Object.fromEntries(values)), { mode: 0o600 });
  renameSync(temporaryPath, path);
}

const states = new Map<string, NodeSavedState>();
const sessions = loadPersistentMap<NodeSavedSession>(oauthSessionsPath);
export const oauth = new NodeOAuthClient({
  clientMetadata,
  stateStore: {
    async set(key, value) { states.set(key, value); },
    async get(key) { return states.get(key); },
    async del(key) { states.delete(key); },
  },
  sessionStore: {
    async set(key, value) {
      sessions.set(key, value);
      savePersistentMap(oauthSessionsPath, sessions);
    },
    async get(key) { return sessions.get(key); },
    async del(key) {
      sessions.delete(key);
      savePersistentMap(oauthSessionsPath, sessions);
    },
  },
  allowHttp: true,
});

export type OAuthSession = NonNullable<Awaited<ReturnType<typeof oauth.restore>>>;

const jwtKeys = await generateKeyPair("ES256");
const jwtPublicKey = await exportJWK(jwtKeys.publicKey);
const jwtKid = "local-dev";

export const jazzJwks = {
  keys: [{ ...jwtPublicKey, kid: jwtKid, alg: "ES256", use: "sig" }],
};

export function jazzToken(did: string) {
  return new SignJWT({ sub: did, claims: { did } })
    .setProtectedHeader({ alg: "ES256", kid: jwtKid })
    .setIssuer("bluesky-offline-react")
    .setIssuedAt()
    .setExpirationTime("15m")
    .sign(jwtKeys.privateKey);
}

export async function currentSession(did: string | undefined) {
  if (!did) return null;
  try { return await oauth.restore(did); } catch { return null; }
}
