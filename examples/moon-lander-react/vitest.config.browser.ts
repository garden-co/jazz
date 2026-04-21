import { createServer } from "node:net";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import {
  openIsolatedApp,
  readIsolatedAttr,
  waitForIsolatedAttr,
  pressIsolatedKey,
  releaseIsolatedKey,
  closeIsolatedApp,
  debugIsolatedState,
  startFreshTestServer,
  stopFreshTestServer,
} from "./tests/browser/commands.js";

/**
 * Find a free port and persist it to a temp file so that both the vitest
 * config (which may be evaluated multiple times in separate processes) and
 * the globalSetup agree on the same port.
 */
const PORT_FILE = join(tmpdir(), "jazz-moon-lander-test-port");

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

async function getTestPort(): Promise<number> {
  // If the port file already exists and is fresh (written within the last 10s),
  // reuse it — this is a second invocation of the config in the same test run.
  try {
    const content = readFileSync(PORT_FILE, "utf8").trim();
    const port = parseInt(content, 10);
    if (port > 0) return port;
  } catch {
    // File doesn't exist yet — first invocation.
  }
  const port = await findFreePort();
  writeFileSync(PORT_FILE, String(port));
  return port;
}

export default defineConfig(async () => {
  const testPort = await getTestPort();
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
        provider: playwright(),
        instances: [{ browser: "chromium", headless: true }],
        commands: {
          openIsolatedApp,
          readIsolatedAttr,
          waitForIsolatedAttr,
          pressIsolatedKey,
          releaseIsolatedKey,
          closeIsolatedApp,
          debugIsolatedState,
          startFreshTestServer,
          stopFreshTestServer,
        },
      },
      include: ["tests/browser/**/*.test.{ts,tsx}"],
      globalSetup: ["tests/browser/global-setup.ts"],
      testTimeout: 30000,
    },
  };
});
