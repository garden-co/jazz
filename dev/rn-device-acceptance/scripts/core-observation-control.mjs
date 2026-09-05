import { createServer } from "node:http";

/** Test-only acknowledgement channel. It never writes Jazz data: a matching
 * installed fixture can proceed only after the independent Core reader sees
 * the device's run marker. Native code supplies all identity fields. */
export async function startCoreObservationControl({ session, expected, host }) {
  const status = {
    requests: 0,
    identityRejected: 0,
    coreWaitStarted: 0,
    coreWaitSucceeded: 0,
    coreWaitFailed: 0,
    acknowledgementsFinished: 0,
    responsesClosedEarly: 0,
  };
  const server = createServer(async (request, response) => {
    response.once("finish", () => {
      if (response.statusCode === 204) status.acknowledgementsFinished++;
    });
    response.once("close", () => {
      if (!response.writableFinished) status.responsesClosedEarly++;
    });
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
      status.requests++;
      if (
        ["platform", "deviceIdentifier", "buildFingerprint", "runNonce"].some(
          (key) =>
            typeof expected[key] !== "string" ||
            !expected[key] ||
            identity?.[key] !== expected[key],
        )
      ) {
        status.identityRejected++;
        response.writeHead(403).end();
        return;
      }
      status.coreWaitStarted++;
      try {
        await session.waitForCoreObservation();
        status.coreWaitSucceeded++;
      } catch {
        status.coreWaitFailed++;
        throw new Error("Core observation was unavailable");
      }
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
    diagnostic() {
      // Keep timeout evidence bounded and structural. Request bodies contain
      // device identity/run data and must never enter CI diagnostics.
      return [
        `requests=${status.requests}`,
        `identityRejected=${status.identityRejected}`,
        `coreWaitStarted=${status.coreWaitStarted}`,
        `coreWaitSucceeded=${status.coreWaitSucceeded}`,
        `coreWaitFailed=${status.coreWaitFailed}`,
        `acknowledgementsFinished=${status.acknowledgementsFinished}`,
        `responsesClosedEarly=${status.responsesClosedEarly}`,
      ].join(",");
    },
  };
}
