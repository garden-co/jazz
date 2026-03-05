import { createServer } from "node:net";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";

/** Find a free port by briefly binding to port 0. */
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
  const testPort = await findFreePort();
  process.env.TEST_PORT = String(testPort);

  return {
    plugins: [wasm(), topLevelAwait(), react()],
    worker: {
      plugins: () => [wasm(), topLevelAwait()],
    },
    define: {
      __TEST_PORT__: testPort,
    },
    test: {
      browser: {
        enabled: true,
        name: "chromium",
        provider: "playwright",
        headless: true,
      },
      include: ["tests/browser/**/*.test.tsx"],
      globalSetup: ["tests/browser/global-setup.ts"],
      testTimeout: 30000,
    },
  };
});
