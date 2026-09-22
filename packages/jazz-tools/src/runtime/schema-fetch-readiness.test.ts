import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { expect, it } from "vitest";
import { fetchSchemaHashes } from "./schema-fetch.js";

for (const status of [404, 401, 403, 400, 503]) {
  it(`reports schema catalogue HTTP ${status} without automatic retries`, async () => {
    const requests: { path: string | undefined; method: string | undefined; secret: unknown }[] =
      [];
    const server = createServer((request, response) => {
      requests.push({
        path: request.url,
        method: request.method,
        secret: request.headers["x-jazz-admin-secret"],
      });
      response.writeHead(status);
      response.end();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    try {
      const address = server.address() as AddressInfo;
      const error = await fetchSchemaHashes(`http://127.0.0.1:${address.port}`, {
        appId: "synthetic-app",
        adminSecret: "synthetic-secret",
      }).then(
        () => {
          throw new Error("Expected HTTP failure");
        },
        (error: Error) => error,
      );
      expect(error.message).toContain(`Schema hashes fetch failed: ${status}`);
      expect(error.message).not.toContain("synthetic-secret");
      if (status === 404) {
        expect(error.message).toContain("Check the server URL and app ID");
        expect(error.message).toContain("healthy and synced, then retry deployment");
        expect(error.message).toContain("A 404 alone does not confirm startup");
      } else {
        expect(error.message).not.toContain("retry deployment");
      }
      expect(requests).toEqual([
        { path: "/apps/synthetic-app/schemas", method: "GET", secret: "synthetic-secret" },
      ]);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }
  });
}
