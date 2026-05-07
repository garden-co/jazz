import { createServer } from "node:net";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { TEST_APP_ID } from "./tests/browser/test-constants.js";

const KID = "auth-simple-chat-test-key";

function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

export default defineConfig(async () => {
  const { publicKey, privateKey } = await generateKeyPair("ES256", { extractable: true });
  const publicJwk = { ...(await exportJWK(publicKey)), kid: KID, use: "sig", alg: "ES256" };

  async function mintJwt(role: string, sub: string): Promise<string> {
    return new SignJWT({ claims: { role } })
      .setProtectedHeader({ alg: "ES256", kid: KID })
      .setSubject(sub)
      .setIssuedAt()
      .setExpirationTime("1h")
      .sign(privateKey);
  }

  const adminJwt = await mintJwt("admin", "admin-test-user");
  const memberJwt = await mintJwt("member", "member-test-user");

  const jwksPort = await findFreePort();
  const jazzPort = await findFreePort();

  process.env.JAZZ_TEST_JWKS_PUBLIC_KEY = JSON.stringify(publicJwk);
  process.env.JAZZ_TEST_JWKS_PORT = String(jwksPort);
  process.env.JAZZ_TEST_JAZZ_PORT = String(jazzPort);

  return {
    plugins: [wasm(), topLevelAwait(), react()],
    worker: {
      plugins: () => [wasm(), topLevelAwait()],
    },
    define: {
      __JAZZ_SERVER_URL__: JSON.stringify(`http://127.0.0.1:${jazzPort}`),
      __APP_ID__: JSON.stringify(TEST_APP_ID),
      __ADMIN_JWT__: JSON.stringify(adminJwt),
      __MEMBER_JWT__: JSON.stringify(memberJwt),
    },
    test: {
      browser: {
        enabled: true,
        provider: playwright(),
        instances: [{ browser: "chromium", headless: true }],
      },
      include: ["tests/browser/**/*.test.{ts,tsx}"],
      globalSetup: ["tests/browser/global-setup.ts"],
      testTimeout: 30000,
    },
  };
});
