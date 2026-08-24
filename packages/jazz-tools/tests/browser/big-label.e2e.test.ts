import { afterEach, describe, expect, it } from "vitest";
import { createDb, generateAuthSecret, type Db } from "../../src/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { TestCleanup, uniqueDbName, waitForCondition } from "./support.js";
import { getJazzServerInfo, getJazzServerJwtForUser } from "./testing-server.js";
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
    const { appId, serverUrl, adminSecret } = await browserTopologyPhase(
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
    // Backend provisioning still needs a concrete local identity so the
    // browser worker can complete its authenticated transport bootstrap.
    const core = await browserTopologyPhase("open authenticated provisioning edge", () =>
      openDb({
        appId,
        serverUrl,
        adminSecret,
        secret: generateAuthSecret(),
        label: "big-label-core",
      }),
    );
    const org = await core
      .insert(app.organizations, { name: "Night Shift", slug: "night-shift" })
      .wait({ tier: "edge" });
    const adminPerson = core.insert(app.people, { userId: "admin", name: "Admin" }).value;
    const editorPerson = core.insert(app.people, { userId: "editor", name: "Editor" }).value;
    const adminMembership = core.insert(app.memberships, {
      organizationId: org.id,
      personId: adminPerson.id,
      userId: "admin",
      role: "admin",
    }).value;
    await core
      .insert(app.memberships, {
        organizationId: org.id,
        personId: editorPerson.id,
        userId: "editor",
        role: "editor",
      })
      .wait({ tier: "edge" });
    const [adminToken, editorToken] = await Promise.all([
      getJazzServerJwtForUser("admin", {}, appId),
      getJazzServerJwtForUser("editor", {}, appId),
    ]);
    const writer = await openDb({
      appId,
      serverUrl,
      jwtToken: adminToken,
      label: "big-label-writer",
    });
    const reader = await openDb({
      appId,
      serverUrl,
      jwtToken: editorToken,
      label: "big-label-reader",
    });
    await Promise.all([
      writer
        .insert(app.artists, {
          organizationId: org.id,
          name: "Aster",
          genre: "ambient",
          status: "active",
        })
        .wait({ tier: "edge" }),
      reader.all(app.artists.where({ organizationId: org.id }), { tier: "edge" }),
    ]);
    const snapshots: string[][] = [];
    ctx.trackSubscription(
      reader.subscribeAll(
        app.artists.where({ organizationId: org.id }).orderBy("name", "asc"),
        (delta) => {
          snapshots.push(delta.all.map((artist) => artist.name));
        },
      ),
    );
    const artist = await writer
      .insert(app.artists, {
        organizationId: org.id,
        name: "Blue Hour",
        genre: "jazz",
        status: "active",
      })
      .wait({ tier: "edge" });
    const release = await writer
      .insert(app.releases, {
        organizationId: org.id,
        artistId: artist.id,
        title: "First Light",
        releaseDate: new Date("2026-01-01"),
        status: "scheduled",
      })
      .wait({ tier: "edge" });
    await writer
      .insert(app.releaseAssignments, {
        organizationId: org.id,
        releaseId: release.id,
        membershipId: adminMembership.id,
        role: "owner",
      })
      .wait({ tier: "edge" });
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
