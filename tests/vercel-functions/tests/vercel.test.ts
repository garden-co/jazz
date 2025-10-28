import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { describe, it, expect, onTestFinished } from "vitest";
import { execa } from "execa";

// @ts-ignore
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const projectRoot = join(packageRoot, "../..");

describe("GET /api/hello", () => {
  async function setupServer() {
    // Start the dev server
    const server = execa(
      join(projectRoot, "node_modules/.bin/next"),
      ["dev", "--turbopack"],
      {
        cwd: packageRoot,
      },
    );
    // Wait for server to be ready
    const url = await new Promise<URL>((resolve, reject) => {
      let url: string;
      server.stdout?.on("data", (data) => {
        console.log("stdout server:", data.toString());

        if (data.toString().includes("- Local:")) {
          url = data
            .toString()
            .split("- Local:")
            .filter(Boolean)[1]
            .split("\n")[0]
            .trim();
        }

        if (data.toString().includes("Ready in")) {
          resolve(new URL(url));
        }
      });

      server.stderr?.on("data", (data) => {
        console.log("error:", data.toString());
      });

      // Reject if server fails to start within 10 seconds
      setTimeout(() => {
        reject(new Error("Server failed to start within timeout"));
      }, 10000);
    });

    onTestFinished(() => {
      // Ensure server is killed after all tests
      server.kill();
    });
    return { server, url };
  }

  it("WASM should work", async () => {
    const { url, server } = await setupServer();

    server.stderr?.on("data", (data) => {
      const message = data.toString();
      // Next.js emits only a warning
      expect(message.includes("DynamicWasmCodeGenerationWarning")).toBe(false);
    });

    const resp = await fetch(`${url.href}api/hello`);
    const data = await resp.json();

    expect(data).toEqual({
      isWasmCrypto: true,
      text: "Hello world!",
    });
  });
});
