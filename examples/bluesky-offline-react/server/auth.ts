import {
  buildAtprotoLoopbackClientMetadata,
  isExpectedSessionError,
  NodeOAuthClient,
} from "@atproto/oauth-client-node";
import { importJWK, SignJWT } from "jose";
import { authenticationDb } from "./jazz.js";
import {
  createBffSessionStore,
  createOAuthSessionStore,
  createOAuthStateStore,
} from "./oauth-session-store.js";
import { jazzJwt, loadOrCreateJazzSigningKeys } from "./signing-keys.js";

export const oauthScope = "atproto transition:generic";
const clientMetadata = buildAtprotoLoopbackClientMetadata({
  redirect_uris: ["http://127.0.0.1:3001/api/auth/callback"],
  scope: oauthScope,
});
export const oauth = new NodeOAuthClient({
  clientMetadata,
  stateStore: createOAuthStateStore(authenticationDb),
  sessionStore: createOAuthSessionStore(authenticationDb),
  // This example uses an HTTP loopback client during local development.
  allowHttp: true,
});

export type OAuthSession = NonNullable<Awaited<ReturnType<typeof oauth.restore>>>;
export const bffSessionCookie = "bff-session";

const bffSessions = createBffSessionStore(authenticationDb);

export function createBffSession(did: string) {
  return bffSessions.create(did);
}

export function invalidateBffSession(sessionId: string) {
  return bffSessions.invalidate(sessionId);
}

const jwtKeys = await loadOrCreateJazzSigningKeys(authenticationDb);
const jwtPrivateKey = await importJWK(jwtKeys.privateJwk, jazzJwt.algorithm);

export const jazzJwks = {
  keys: [
    {
      ...jwtKeys.publicJwk,
      kid: jazzJwt.keyId,
      alg: jazzJwt.algorithm,
      use: "sig",
    },
  ],
};

export function createJazzToken(did: string) {
  return new SignJWT({ sub: did })
    .setProtectedHeader({ alg: jazzJwt.algorithm, kid: jazzJwt.keyId })
    .setIssuer(jazzJwt.issuer)
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
