import { exportJWK, generateKeyPair, type JWK } from "jose";
import type { EncryptedValueStore } from "./oauth-session-store.js";

export type StoredJwtKeys = {
  privateJwk: JWK;
  publicJwk: JWK;
};

export async function loadOrCreateJwtKeys(
  store: EncryptedValueStore<StoredJwtKeys>,
) {
  const stored = await store.get("es256");
  if (stored) return stored;

  const generated = await generateKeyPair("ES256", { extractable: true });
  const keys = {
    privateJwk: await exportJWK(generated.privateKey),
    publicJwk: await exportJWK(generated.publicKey),
  };
  await store.set("es256", keys);
  return keys;
}
