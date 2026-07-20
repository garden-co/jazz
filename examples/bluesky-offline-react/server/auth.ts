import {
  buildAtprotoLoopbackClientMetadata,
  isExpectedSessionError,
  NodeOAuthClient,
} from "@atproto/oauth-client-node";
import {
  importJWK,
  SignJWT,
} from "jose";
import { db } from "./jazz.js";
import {
  createBffSessionStore,
  createEncryptedValueStore,
  createOAuthSessionStore,
  createOAuthStateStore,
} from "./oauth-session-store.js";
import { loadOrCreateJwtKeys, type StoredJwtKeys } from "./signing-keys.js";

export const oauthScope = "atproto transition:generic";
const clientMetadata = buildAtprotoLoopbackClientMetadata({
  redirect_uris: ["http://127.0.0.1:3001/api/auth/callback"],
  scope: oauthScope,
});
export const oauth = new NodeOAuthClient({
  clientMetadata,
  stateStore: createOAuthStateStore(db),
  sessionStore: createOAuthSessionStore(db),
  // This example uses an HTTP loopback client during local development.
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

export function createJazzToken(did: string) {
  return new SignJWT({ sub: did })
    .setProtectedHeader({ alg: "ES256", kid: jwtKid })
    .setIssuer("bluesky-offline-react")
    .setIssuedAt()
    .setExpirationTime("15m")
    .sign(jwtPrivateKey);
}

export async function restoreBffSession(sessionId: string | undefined) {
  if (!sessionId) return null;
  const did = await bffSessions.resolve(sessionId);
  if (!did) return null;
  try {
    return { did, session: await oauth.restore(did) };
  } catch (error) {
    if (isExpectedSessionError(error)) {
      await bffSessions.invalidate(sessionId);
    }
    return null;
  }
}
