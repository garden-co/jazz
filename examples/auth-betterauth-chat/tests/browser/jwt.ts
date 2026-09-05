import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";

const KID = "auth-betterauth-chat-test-key";
const ISSUER = "http://127.0.0.1:3000";

export interface TestKeySet {
  publicJwk: JWK;
  mintJwt(sub: string, role: "admin" | "member"): Promise<string>;
}

export async function createTestKeySet(): Promise<TestKeySet> {
  const { publicKey, privateKey } = await generateKeyPair("ES256", { extractable: true });
  const publicJwk: JWK = {
    ...(await exportJWK(publicKey)),
    kid: KID,
    use: "sig",
    alg: "ES256",
  };

  async function mintJwt(sub: string, role: "admin" | "member"): Promise<string> {
    return new SignJWT({ role })
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setSubject(sub)
      .setIssuer(ISSUER)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  return { publicJwk, mintJwt };
}
