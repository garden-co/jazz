import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";

const KID = "auth-betterauth-chat-test-key";

export interface TestKeySet {
  publicJwk: JWK;
  mintJwt(sub: string): Promise<string>;
}

export async function createTestKeySet(): Promise<TestKeySet> {
  const { publicKey, privateKey } = await generateKeyPair("ES256", { extractable: true });
  const publicJwk: JWK = {
    ...(await exportJWK(publicKey)),
    kid: KID,
    use: "sig",
    alg: "ES256",
  };

  // The Jazz permission only checks `authMode: "external"`, which is satisfied
  // by any verified JWT. Better Auth's role/identity claims aren't read by the
  // policy, so a minimal JWT with just `sub` is enough to exercise the rules.
  async function mintJwt(sub: string): Promise<string> {
    return new SignJWT({})
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setSubject(sub)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  return { publicJwk, mintJwt };
}
