import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { execa } from "execa";
import { expect, test } from "vitest";

// @ts-ignore
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const projectRoot = join(packageRoot, "../..");

test("server responds with hello world", async () => {
  // Start the dev server
  const server = execa(
    join(projectRoot, "node_modules/.bin/wrangler"),
    ["dev"],
    {
      cwd: packageRoot,
    },
  );

  try {
    // Wait for server to be ready
    const url = await new Promise<URL>((resolve, reject) => {
      server.stdout?.on("data", (data) => {
        if (data.toString().includes("Ready on http://localhost:")) {
          resolve(new URL(data.toString().split("Ready on ")[1].trim()));
        }
      });

      // Reject if server fails to start within 10 seconds
      setTimeout(() => {
        reject(new Error("Server failed to start within timeout"));
      }, 10000);
    });

    // Make request to server
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(
        `Server returned status ${response.status}. Response: ${await response.text()}`,
      );
    }

    const data = await response.json();

    // Verify response
    expect(data).toEqual({ text: "Hello world!" });
  } finally {
    // Ensure server is killed even if test fails
    server.kill();
  }
});
