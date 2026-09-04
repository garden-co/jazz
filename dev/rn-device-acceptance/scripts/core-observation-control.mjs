import { createServer } from "node:http";

/** Test-only acknowledgement channel. It never writes Jazz data: a matching
 * installed fixture can proceed only after the independent Core reader sees
 * the device's run marker. Native code supplies all identity fields. */
export async function startCoreObservationControl({ session, expected, host }) {
  const server = createServer(async (request, response) => {
    try {
      if (request.method !== "POST" || request.url !== "/core-observation") {
        response.writeHead(404).end();
        return;
      }
      let body = "";
      for await (const chunk of request) {
        body += chunk;
        if (Buffer.byteLength(body) > 4096) {
          response.writeHead(413).end();
          return;
        }
      }
      const identity = JSON.parse(body);
      if (
        ["platform", "deviceIdentifier", "buildFingerprint", "runNonce"].some(
          (key) =>
            typeof expected[key] !== "string" ||
            !expected[key] ||
            identity?.[key] !== expected[key],
        )
      ) {
        response.writeHead(403).end();
        return;
      }
      await session.waitForCoreObservation();
      response.writeHead(204).end();
    } catch {
      response.writeHead(503).end();
    }
  });
  server.requestTimeout = 70_000;
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return {
    endpoint: `http://${host}:${server.address().port}/core-observation`,
    close() {
      server.closeAllConnections();
      return new Promise((resolve) => server.close(resolve));
    },
  };
}
