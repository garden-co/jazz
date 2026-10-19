import { randomUUID } from "node:crypto";
import { createServer } from "node:net";
import { defineConfig, devices } from "@playwright/test";

async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) =>
    server.once("error", reject).listen(0, "127.0.0.1", resolve),
  );
  const address = server.address();
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  if (!address || typeof address === "string") throw new Error("No test server port allocated");
  return address.port;
}

const WEB_PORT = Number(process.env.WEB_PORT ?? (await availablePort()));
process.env.WEB_PORT ??= String(WEB_PORT);
const testAppId = randomUUID();
// Keep each test server independent of ambient credentials. An explicitly empty
// backend secret is invalid; this fresh credential stays in the server process.
const testBackendSecret = randomUUID();

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  testIgnore: "**/config-security.spec.ts",
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  use: {
    baseURL: `http://127.0.0.1:${WEB_PORT}`,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    url: `http://127.0.0.1:${WEB_PORT}`,
    command: `pnpm dev --hostname 127.0.0.1 --port ${WEB_PORT}`,
    timeout: 60_000,
    env: {
      NEXT_PUBLIC_JAZZ_APP_ID: testAppId,
      NEXT_PUBLIC_JAZZ_SERVER_URL: "",
      BACKEND_SECRET: testBackendSecret,
      JAZZ_BACKEND_SECRET: testBackendSecret,
      ADMIN_SECRET: "",
      JAZZ_ADMIN_SECRET: "",
      JAZZ_E2E_IN_MEMORY: "1",
    },
  },
});
