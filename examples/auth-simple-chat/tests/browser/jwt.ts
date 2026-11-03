import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";

const KID = "auth-simple-chat-test-key";
export const TEST_JWT_ISSUER = "https://auth-simple-chat.test/";
export const TEST_JWT_AUDIENCE = "auth-simple-chat";

export interface TestKeySet {
  publicJwk: JWK;
  mintJwt(role: string, sub: string): Promise<string>;
}

export async function createTestKeySet(): Promise<TestKeySet> {
  const { publicKey, privateKey } = await generateKeyPair("ES256", { extractable: true });
  const publicJwk: JWK = {
    ...(await exportJWK(publicKey)),
    kid: KID,
    use: "sig",
    alg: "ES256",
  };

  // Test fixtures enroll with a JWT the local core trusts. The permission
  // policy reads `session.claims.role`, which the app's backend sets from its
  // own session in `authenticate`.
  async function mintJwt(role: string, sub: string): Promise<string> {
    return new SignJWT({ role })
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setIssuer(TEST_JWT_ISSUER)
      .setAudience(TEST_JWT_AUDIENCE)
      .setSubject(sub)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  return { publicJwk, mintJwt };
}
