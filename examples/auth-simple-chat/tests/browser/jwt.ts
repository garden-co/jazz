import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";

const KID = "auth-simple-chat-test-key";

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

  // The auth-simple-chat permission policy reads `session.claims.role` to gate
  // posting to the announcements room. The local Express auth server nests the
  // role under `claims` in the payload it signs, so the test JWTs match.
  async function mintJwt(role: string, sub: string): Promise<string> {
    return new SignJWT({ claims: { role } })
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setSubject(sub)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  return { publicJwk, mintJwt };
}
