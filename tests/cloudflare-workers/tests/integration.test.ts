import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { execa } from "execa";
import { expect, test, describe, onTestFinished } from "vitest";
import { startSyncServer } from "jazz-run/startSyncServer";

// @ts-ignore
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const projectRoot = join(packageRoot, "../..");

describe("Cloudflare Workers Integration Test", () => {
  async function setupServer() {
    // Start the dev server
    const server = execa(
      join(projectRoot, "node_modules/.bin/wrangler"),
      ["dev"],
      {
        cwd: packageRoot,
      },
    );

    const syncServer = await startSyncServer({
      port: "4211",
      inMemory: true,
      db: "sync-db/storage.db",
      host: "127.0.0.1",
    });

    // Wait for server to be ready
    const url = await new Promise<URL>((resolve, reject) => {
      server.stdout?.on("data", (data) => {
        console.log("stdout server:", data.toString());
        if (data.toString().includes("Ready on http://localhost:")) {
          resolve(new URL(data.toString().split("Ready on ")[1].trim()));
        }
      });

      server.stderr?.on("data", (data) => {
        console.log("stderr server:", data.toString());
      });

      // Reject if server fails to start within 10 seconds
      setTimeout(() => {
        reject(new Error("Server failed to start within timeout"));
      }, 10000);
    });

    onTestFinished(() => {
      // Ensure server is killed after all tests
      server.kill();
      syncServer.closeAllConnections();
      syncServer.localNode.gracefulShutdown();
      syncServer.close();
    });
    return { server, url };
  }

  test("server responds with hello world", async () => {
    const { url } = await setupServer();

    // Make request to server
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(
        `Server returned status ${response.status}. Response: ${await response.text()}`,
      );
    }

    const data = await response.json();

    // Verify response
    expect(data.text).toBe("Hello world!");
  });

  test("WASM crypto works", async () => {
    const { url } = await setupServer();

    // Make request to server, the qs is for activate the initialization of the wasm module
    const response = await fetch(`${url}?initWasm=true`);

    if (!response.ok) {
      throw new Error(
        `Server returned status ${response.status}. Response: ${await response.text()}`,
      );
    }

    const data = await response.json();

    // Verify response
    expect(data).toEqual({ text: "Hello world!", isWasmCrypto: true });
  });
});
