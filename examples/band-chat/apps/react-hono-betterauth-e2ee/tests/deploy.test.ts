// Deploying BandChat's generated files: no refusal, and exactly one warning.
// A read marker is sealed with its room's key, so every member of the room
// could decrypt it, while the read rule sends each marker to its reader only.
// That is the accepted trade-off the warning names.
import { afterEach, expect, it } from "vitest";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "jazz-tools/testing";
import { app } from "../jazz.generated";
import permissions from "../jazz.permissions.json" with { type: "json" };

let server: LocalJazzServerHandle | undefined;
afterEach(async () => {
  await server?.stop();
  server = undefined;
});

it("deploys with exactly the read-marker warning", async () => {
  server = await startLocalJazzServer();
  const result = await deploy({
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
    app,
    permissions,
  });
  expect(result.schema.status).toBe("published");
  expect(result.warnings).toEqual([
    "jazz.permissions.json: table readMarkers: its read rule is its space table rooms's plus further conditions, " +
      "so every member of a space can decrypt rows of readMarkers that Jazz doesn't send them",
  ]);
}, 60_000);
