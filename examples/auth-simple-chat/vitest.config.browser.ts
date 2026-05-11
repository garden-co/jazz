import { createServer } from "node:net";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { createTestKeySet } from "./tests/browser/jwt.js";
import { TEST_APP_ID } from "./tests/browser/test-constants.js";

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
  const { publicJwk, mintJwt } = await createTestKeySet();
  const adminJwt = await mintJwt("admin", "admin-test-user");
  const memberJwt = await mintJwt("member", "member-test-user");

  const jwksPort = await findFreePort();
  const jazzPort = await findFreePort();

  // Vitest doesn't pass state from config to global-setup directly; the only
  // channel is process.env. global-setup.ts reads these via requireEnv() to
  // boot the JWKS server and the Jazz TestingServer.
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
