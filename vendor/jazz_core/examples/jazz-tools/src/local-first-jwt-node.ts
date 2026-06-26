import {
  Buffer,
} from "node:buffer";
import {
  createPrivateKey,
  createPublicKey,
  sign as signBytes,
} from "node:crypto";
import {
  createLocalFirstJwtWithSigner,
  localFirstPrivateKeyDer,
  type LocalFirstJwtOptions,
} from "./local-first-jwt-core.js";

export function createLocalFirstJwt(options: LocalFirstJwtOptions): string {
  return createLocalFirstJwtWithSigner(options, (signingInput, privateKeyDer) => {
    return signBytes(null, signingInput, {
      key: Buffer.from(privateKeyDer),
      format: "der",
      type: "pkcs8",
    });
  });
}

export function localFirstJwtPublicKeyPem(appId: string, secret: string): string {
  const privateKey = createPrivateKey({
    key: Buffer.from(localFirstPrivateKeyDer(appId, secret)),
    format: "der",
    type: "pkcs8",
  });
  return createPublicKey(privateKey).export({ format: "pem", type: "spki" }).toString();
}
