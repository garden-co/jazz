import {
  buildAtprotoLoopbackClientMetadata,
  NodeOAuthClient,
  type NodeSavedState,
} from "@atproto/oauth-client-node";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { db } from "./jazz.js";
import { createOAuthSessionStore } from "./oauth-session-store.js";

const port = Number(process.env.PORT ?? 3001);
export const oauthScope = "atproto transition:generic";
const clientMetadata = buildAtprotoLoopbackClientMetadata({
  redirect_uris: [`http://127.0.0.1:${port}/api/auth/callback`],
  scope: oauthScope,
});

const states = new Map<string, NodeSavedState>();
export const oauth = new NodeOAuthClient({
  clientMetadata,
  stateStore: {
    async set(key, value) { states.set(key, value); },
    async get(key) { return states.get(key); },
    async del(key) { states.delete(key); },
  },
  sessionStore: createOAuthSessionStore(db),
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
