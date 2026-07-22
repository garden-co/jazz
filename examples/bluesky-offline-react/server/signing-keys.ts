import { exportJWK, generateKeyPair, type JWK } from "jose";
import type { Db } from "jazz-tools";
import { createEncryptedValueStore } from "./oauth-session-store.js";

export const jazzJwt = {
  algorithm: "ES256",
  issuer: "bluesky-offline-react",
  keyId: "local-dev",
} as const;

export type JazzSigningKeyPair = {
  privateJwk: JWK;
  publicJwk: JWK;
};

const signingKeyNamespace = "jazz-signing-key:";
const signingKeyName = "es256";

export async function loadOrCreateJazzSigningKeys(database: Db) {
  const store = createEncryptedValueStore<JazzSigningKeyPair>(database, signingKeyNamespace);
  const stored = await store.get(signingKeyName);
  if (stored) return stored;

  const generated = await generateKeyPair(jazzJwt.algorithm, {
    extractable: true,
  });
  const keys = {
    privateJwk: await exportJWK(generated.privateKey),
    publicJwk: await exportJWK(generated.publicKey),
  };

  // Persisting the pair keeps issued Jazz tokens verifiable across BFF restarts.
  await store.set(signingKeyName, keys);
  return keys;
}
