import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { appArtifact } from "../testing/app-artifact.js";
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

describe("deployApp refuses a jazz.app.json the app's code doesn't declare", () => {
  it("names schema.ts when the file's current version isn't the app's schema", async () => {
    const draft = s.defineApp(notesV2, { migrations: "none" });
    const permissions = readable(draft);
    // The file still describes v1: schema.ts changed and validate didn't run.
    const app = s.defineApp(notesV2, {
      artifact: await appArtifact(notesV1, {
        permissions: readable(s.defineApp(notesV1, { migrations: "none" })),
      }),
    });
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.toThrow(
      /jazz\.app\.json is out of date with schema\.ts: the app's schema is [0-9a-f]{64}, but the file's current schema is [0-9a-f]{64}\. .*npx jazz-tools validate/,
    );
  });

  it("names permissions.ts when the permissions given don't make the file's bundle", async () => {
    const draft = s.defineApp(notesV1, { migrations: "none" });
    const app = s.defineApp(notesV1, {
      artifact: await appArtifact(notesV1, { permissions: readable(draft) }),
    });
    // permissions.ts now also lets anyone insert; the file wasn't regenerated.
    const edited = s.definePermissions(draft, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
    });
    await expect(deployApp({ ...unreachable, app, permissions: edited })).rejects.toThrow(
      /jazz\.app\.json is out of date with permissions\.ts: the permissions createJazz was given make bundle [0-9a-f]{64}, but the file holds bundle [0-9a-f]{64}\. .*npx jazz-tools validate/,
    );
  });

  it("reaches the server once the file matches", async () => {
    const draft = s.defineApp(notesV1, { migrations: "none" });
    const permissions = readable(draft);
    const app = s.defineApp(notesV1, { artifact: await appArtifact(notesV1, { permissions }) });
    // Past both checks, the publish itself fails on the closed port.
    await expect(deployApp({ ...unreachable, app, permissions })).rejects.not.toThrow(
      /out of date/,
    );
  });
});
