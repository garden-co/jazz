import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../src/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { TestCleanup, uniqueDbName, waitForCondition } from "./support.js";
import {
  bootstrapBigLabelOrganization,
  getJazzServerInfo,
  getJazzServerJwtForUser,
} from "./testing-server.js";
import { browserTopologyPhase } from "./topology-harness.js";

const ctx = new TestCleanup();
afterEach(async () => ctx.cleanup());

describe("BigLabel browser edge/core topology", () => {
  it("converges an ordered artist-release roster through two authenticated peer edges", async () => {
    const [{ app }, permissions] = await browserTopologyPhase("load BigLabel adopter modules", () =>
      Promise.all([
        import("../../../../examples/big-label/schema.js"),
        import("../../../../examples/big-label/permissions.js").then((module) => module.default),
      ]),
    );
    const { appId, serverUrl, adminSecret, backendSecret } = await browserTopologyPhase(
      "start BigLabel core",
      () => getJazzServerInfo(uniqueDbName("big-label-topology")),
    );
    await browserTopologyPhase("publish BigLabel authority", async () => {
      const { hash } = await publishStoredSchema(serverUrl, {
        appId,
        adminSecret,
        schema: app.wasmSchema,
      });
      const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
      await publishStoredPermissions(serverUrl, {
        appId,
        adminSecret,
        schemaHash: hash,
        permissions,
        expectedParentBundleObjectId: head?.bundleObjectId ?? null,
      });
    });
    const org = await browserTopologyPhase(
      "bootstrap BigLabel admin through backend authority",
      () =>
        bootstrapBigLabelOrganization(
          { appId, serverUrl, adminSecret, backendSecret },
          "admin",
          "Admin",
        ),
    );
    // A user legitimately provisions their own person record before an
    // organization admin admits them to this label. This keeps the role grant
    // on the same public policy path used by the app instead of making a test
    // authority bypass.
    await browserTopologyPhase("bootstrap BigLabel editor through backend authority", () =>
      bootstrapBigLabelOrganization(
        { appId, serverUrl, adminSecret, backendSecret },
        "editor",
        "Editor",
      ),
    );
    const [adminToken, editorToken] = await Promise.all([
      getJazzServerJwtForUser("admin", {}, appId),
      getJazzServerJwtForUser("editor", {}, appId),
    ]);
    const writer = await browserTopologyPhase("open authenticated admin edge", () =>
      openDb({
        appId,
        serverUrl,
        jwtToken: adminToken,
        label: "big-label-writer",
      }),
    );
    const adminMembership = await browserTopologyPhase(
      "read bootstrapped admin membership",
      async () => {
        const memberships = await writer.all(app.memberships.where({ organizationId: org.id }), {
          tier: "edge",
        });
        expect(memberships).toHaveLength(1);
        return memberships[0]!;
      },
    );
    const editorPerson = await browserTopologyPhase("read bootstrapped editor person", async () => {
      const people = await writer.all(app.people.where({ userId: "editor" }), { tier: "edge" });
      expect(people).toHaveLength(1);
      return people[0]!;
    });
    await browserTopologyPhase("admit editor through admin membership grant", async () => {
      await writer
        .insert(app.memberships, {
          organizationId: org.id,
          personId: editorPerson.id,
          userId: "editor",
          role: "editor",
        })
        .wait({ tier: "edge" });
    });
    const reader = await browserTopologyPhase("open authenticated editor edge", () =>
      openDb({
        appId,
        serverUrl,
        jwtToken: editorToken,
        label: "big-label-reader",
      }),
    );
    await browserTopologyPhase("writer seeds initial artist", async () => {
      await writer
        .insert(app.artists, {
          organizationId: org.id,
          name: "Aster",
          genre: "ambient",
          status: "active",
        })
        .wait({ tier: "edge" });
    });
    await browserTopologyPhase("editor reads admitted organization roster", () =>
      reader.all(app.artists.where({ organizationId: org.id }), { tier: "edge" }),
    );
    const snapshots: string[][] = [];
    await browserTopologyPhase("start editor ordered-roster subscription", () => {
      ctx.trackSubscription(
        reader.subscribeAll(
          app.artists.where({ organizationId: org.id }).orderBy("name", "asc"),
          (delta) => {
            snapshots.push(delta.all.map((artist) => artist.name));
          },
        ),
      );
    });
    const artist = await browserTopologyPhase("writer adds convergent artist", () =>
      writer
        .insert(app.artists, {
          organizationId: org.id,
          name: "Blue Hour",
          genre: "jazz",
          status: "active",
        })
        .wait({ tier: "edge" }),
    );
    const release = await browserTopologyPhase("writer adds artist release", () =>
      writer
        .insert(app.releases, {
          organizationId: org.id,
          artistId: artist.id,
          title: "First Light",
          releaseDate: new Date("2026-01-01"),
          status: "scheduled",
        })
        .wait({ tier: "edge" }),
    );
    await browserTopologyPhase("writer assigns release owner", () =>
      writer
        .insert(app.releaseAssignments, {
          organizationId: org.id,
          releaseId: release.id,
          membershipId: adminMembership.id,
          role: "owner",
        })
        .wait({ tier: "edge" }),
    );
    await browserTopologyPhase(
      "reader subscription converges ordered roster",
      () =>
        waitForCondition(
          async () => snapshots.some((rows) => rows.includes("Blue Hour")),
          15_000,
          "reader did not receive BigLabel artist",
        ),
      20_000,
    );
    expect(snapshots.at(-1)).toContain("Blue Hour");
  }, 60_000);
});

async function openDb(input: {
  appId: string;
  serverUrl: string;
  adminSecret?: string;
  secret?: string;
  jwtToken?: string;
  label: string;
}): Promise<Db> {
  return ctx.track(
    await createDb({
      appId: input.appId,
      serverUrl: input.serverUrl,
      ...(input.adminSecret
        ? { adminSecret: input.adminSecret, secret: input.secret! }
        : { jwtToken: input.jwtToken! }),
      driver: { type: "persistent", dbName: uniqueDbName(input.label) },
    }),
  );
}
