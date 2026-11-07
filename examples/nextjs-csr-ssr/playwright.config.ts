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
      // Each test server keeps its data in memory.
      JAZZ_IN_MEMORY: "1",
    },
  },
});
