import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { appArtifact, appPermissions } from "../testing/app-artifact.js";
import { deployApp } from "./catalogue.js";

// Nothing listens here: each refusal must come before the deployment is sent.
const unreachable = {
  appId: "00000000-0000-0000-0000-000000000001",
  serverUrl: "http://127.0.0.1:9",
  adminSecret: "unused",
};

const notesV1 = { notes: s.table({ body: s.string() }, {}) };
const notesV2 = { notes: s.table({ body: s.string(), done: s.boolean().optional() }, {}) };

function readable(app: object) {
  return s.definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
  });
}

describe("deployApp refuses files the app's code doesn't declare", () => {
  it("names schema.ts when jazz.app.json's current version isn't the app's schema", async () => {
    // The files still describe v1: schema.ts changed and validate didn't run.
    const app = s.defineApp(notesV2, { artifact: await appArtifact(notesV1) });
    const permissions = await appPermissions(notesV1, readable(app));
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.toThrow(
      /jazz\.app\.json is out of date with schema\.ts: the app's schema is [0-9a-f]{64}, but the file's current schema is [0-9a-f]{64}\. .*npx jazz-tools validate/,
    );
  });

  it("refuses a jazz.permissions.json that wasn't written with the app's jazz.app.json", async () => {
    const app = s.defineApp(notesV2, { artifact: await appArtifact(notesV2) });
    // validate ran for v1's permissions; jazz.app.json is v2's.
    const permissions = await appPermissions(notesV1, readable(app));
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.toThrow(
      /jazz\.permissions\.json: `schema` is [0-9a-f]{64} but jazz\.app\.json's current schema is [0-9a-f]{64}: the two files were not written together/,
    );
  });

  it("refuses definePermissions' map for an app that ships its jazz.app.json", async () => {
    const app = s.defineApp(notesV1, { artifact: await appArtifact(notesV1) });
    await expect(deployApp({ ...unreachable, app, permissions: readable(app) })).rejects.toThrow(
      /takes its permissions from jazz\.permissions\.json, not from definePermissions\(\.\.\.\)\. .*import permissions from "\.\/jazz\.permissions\.json"/,
    );
  });

  it('refuses a jazz.permissions.json for an app on { migrations: "none" }', async () => {
    const app = s.defineApp(notesV1, { migrations: "none" });
    const permissions = await appPermissions(notesV1, readable(app));
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.toThrow(
      /jazz\.permissions\.json is for an app that ships its jazz\.app\.json/,
    );
  });

  it("reaches the server once the files match", async () => {
    const app = s.defineApp(notesV1, { artifact: await appArtifact(notesV1) });
    const permissions = await appPermissions(app, readable(app));
    // Past every check, the publish itself fails on the closed port.
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.not.toThrow(
      /out of date|jazz\.permissions\.json|definePermissions/,
    );
  });
});
