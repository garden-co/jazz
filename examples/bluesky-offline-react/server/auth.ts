import {
  buildAtprotoLoopbackClientMetadata,
  NodeOAuthClient,
} from "@atproto/oauth-client-node";
import {
  importJWK,
  SignJWT,
} from "jose";
import { getBackendDb } from "./jazz.js";
import {
  createBffSessionStore,
  createEncryptedValueStore,
  createOAuthSessionStore,
  createOAuthStateStore,
} from "./oauth-session-store.js";
import { loadOrCreateJwtKeys, type StoredJwtKeys } from "./signing-keys.js";

const port = Number(process.env.PORT ?? 3001);
export const oauthScope = "atproto transition:generic";
const clientMetadata = buildAtprotoLoopbackClientMetadata({
  redirect_uris: [`http://127.0.0.1:${port}/api/auth/callback`],
  scope: oauthScope,
});
const db = getBackendDb();

export const oauth = new NodeOAuthClient({
  clientMetadata,
  stateStore: createOAuthStateStore(db),
  sessionStore: createOAuthSessionStore(db),
  allowHttp: true,
});

export type OAuthSession = NonNullable<Awaited<ReturnType<typeof oauth.restore>>>;
export const bffSessionCookie = "bff-session";

const bffSessions = createBffSessionStore(db);

export function createBffSession(did: string) {
  return bffSessions.create(did);
}

export function invalidateBffSession(sessionId: string) {
  return bffSessions.invalidate(sessionId);
}

const jwtKeyStore = createEncryptedValueStore<StoredJwtKeys>(db, "jazz-signing-key:");
const jwtKeys = await loadOrCreateJwtKeys(jwtKeyStore);
const jwtPrivateKey = await importJWK(jwtKeys.privateJwk, "ES256");
const jwtKid = "local-dev";

export const jazzJwks = {
  keys: [{ ...jwtKeys.publicJwk, kid: jwtKid, alg: "ES256", use: "sig" }],
};

export function jazzToken(did: string) {
  return new SignJWT({ sub: did, claims: { did } })
    .setProtectedHeader({ alg: "ES256", kid: jwtKid })
    .setIssuer("bluesky-offline-react")
    .setIssuedAt()
    .setExpirationTime("15m")
    .sign(jwtPrivateKey);
}

export async function currentSession(sessionId: string | undefined) {
  if (!sessionId) return null;
  const did = await bffSessions.resolve(sessionId);
  if (!did) return null;
  try {
    return { did, session: await oauth.restore(did) };
  } catch {
    await bffSessions.invalidate(sessionId);
    return null;
  }
}
