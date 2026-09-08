import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";
import { WORKOS_CLIENT_ID } from "../../constants.js";

const KID = "auth-workos-chat-test-key";
export const TEST_WORKOS_JWT_ISSUER = "https://workos.test/";

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

  // Jazz reads standard flat JWT application metadata as `session.claims.role`.
  async function mintJwt(role: string, sub: string): Promise<string> {
    return new SignJWT({ role })
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setIssuer(TEST_WORKOS_JWT_ISSUER)
      .setAudience(WORKOS_CLIENT_ID)
      .setSubject(sub)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  return { publicJwk, mintJwt };
}
