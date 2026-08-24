import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db } from "../../src/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { sleep, TestCleanup, uniqueDbName, waitForCondition } from "./support.js";
import {
  blockJazzServerNetwork,
  bootstrapBigLabelOrganization,
  getJazzServerInfo,
  getJazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "./testing-server.js";
import {
  browserTopologyPhase,
  browserTopologyReporter,
  runTopologyScenario,
} from "./topology-harness.js";

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
      "concurrently ensure one BigLabel admin tenant through backend authority",
      async () => {
        const [first, second] = await Promise.all([
          bootstrapBigLabelOrganization(
            { appId, serverUrl, adminSecret, backendSecret },
            "admin",
            "Admin",
          ),
          bootstrapBigLabelOrganization(
            { appId, serverUrl, adminSecret, backendSecret },
            "admin",
            "Admin",
          ),
        ]);
        expect(second.id).toBe(first.id);
        return first;
      },
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
    // Give the editor a second, real tenant before opening their edge. The
    // application queries below deliberately stay scoped to `org`: a dropped
    // `organizationId` constraint would otherwise be invisible because all
    // rows the editor can read happen to belong to the same organization.
    const foreignOrg = await browserTopologyPhase(
      "bootstrap a second BigLabel tenant through backend authority",
      () =>
        bootstrapBigLabelOrganization(
          { appId, serverUrl, adminSecret, backendSecret },
          "foreign-admin",
          "Foreign Admin",
        ),
    );
    const [adminToken, editorToken, foreignAdminToken] = await Promise.all([
      getJazzServerJwtForUser("admin", {}, appId),
      getJazzServerJwtForUser("editor", {}, appId),
      getJazzServerJwtForUser("foreign-admin", {}, appId),
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
      "prove concurrent bootstrap produced one complete tenant graph",
      async () => {
        const organizations = await writer.all(
          app.organizations.where({ slug: "personal-admin" }),
          {
            tier: "edge",
          },
        );
        const people = await writer.all(app.people.where({ userId: "admin" }), { tier: "edge" });
        const memberships = await writer.all(app.memberships.where({ organizationId: org.id }), {
          tier: "edge",
        });
        expect(organizations).toHaveLength(1);
        expect(organizations[0]!.id).toBe(org.id);
        expect(people).toHaveLength(1);
        expect(memberships).toHaveLength(1);
        expect(memberships[0]!.personId).toBe(people[0]!.id);
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
    const foreignWriter = await browserTopologyPhase("open second tenant admin edge", () =>
      openDb({
        appId,
        serverUrl,
        jwtToken: foreignAdminToken,
        label: "big-label-foreign-writer",
      }),
    );
    await browserTopologyPhase("admit editor to the second tenant", async () => {
      const foreignEditor = await foreignWriter.all(
        app.people.where({ userId: "editor" }).limit(1),
        { tier: "edge" },
      );
      expect(foreignEditor).toHaveLength(1);
      await foreignWriter
        .insert(app.memberships, {
          organizationId: foreignOrg.id,
          personId: foreignEditor[0]!.id,
          userId: "editor",
          role: "editor",
        })
        .wait({ tier: "edge" });
    });
    await browserTopologyPhase("seed readable foreign tenant rows", async () => {
      // These values sort before the org-one rows below. They are intentionally
      // authorized for the same editor, so removing either query's
      // `organizationId` predicate makes its bounded result deterministically
      // contain a foreign row rather than merely widening an invisible set.
      const foreignArtist = await foreignWriter
        .insert(app.artists, {
          organizationId: foreignOrg.id,
          name: "Aardvark",
          genre: "ambient",
          status: "active",
        })
        .wait({ tier: "edge" });
      await foreignWriter
        .insert(app.releases, {
          organizationId: foreignOrg.id,
          artistId: foreignArtist.id,
          title: "Foreign Prelude",
          releaseDate: new Date("2025-01-01"),
          status: "scheduled",
        })
        .wait({ tier: "edge" });
    });
    let reader = await browserTopologyPhase("open authenticated editor edge", () =>
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
    const roster = app.artists.where({ organizationId: org.id }).orderBy("name", "asc").limit(2);
    // This is the application's other primary read shape: a bounded release
    // pipeline whose included artist must remain tenant-scoped throughout
    // offline recovery. Keep it separate from the roster so the receipt
    // catches a regression in include delivery even if the base artist query
    // still converges.
    const releasePipeline = app.releases
      .where({ organizationId: org.id })
      .orderBy("releaseDate", "asc")
      .limit(1)
      .include({ artist: true });
    const snapshots: string[][] = [];
    await runTopologyScenario(
      {
        id: "big-label.admin-roster.reconnect-reopen-revocation",
        topology: ["browser", "edge", "core"],
        seed: 1719,
        phaseTimeoutMs: 20_000,
        faultTimeoutMs: 10_000,
        replay: "pnpm --filter jazz-tools test:browser -- big-label.e2e.test.ts",
        targets: {
          "browser-edge": {
            async disconnect() {
              await blockJazzServerNetwork(serverUrl);
              // Route blocking applies only to future WebSocket connections.
              // Close the live reader connection so this phase exercises the
              // actual offline cache path rather than a still-open socket.
              await reader.disconnect();
              await sleep(100);
            },
            async reconnect() {
              await unblockJazzServerNetwork(serverUrl);
              await reader.reconnect();
              await sleep(100);
            },
          },
        },
        phases: [
          {
            name: "editor reads exact bounded roster and subscribes",
            async run() {
              await expect(reader.all(roster, { tier: "edge" })).resolves.toEqual([
                expect.objectContaining({ name: "Aster", organizationId: org.id }),
              ]);
              ctx.trackSubscription(
                reader.subscribeAll(roster, (delta) =>
                  snapshots.push(delta.all.map((artist) => artist.name)),
                ),
              );
            },
            faultsAfter: [{ kind: "disconnect", target: "browser-edge" }],
          },
          {
            name: "admin writes artist and release while editor is offline",
            async run() {
              const artist = await writer
                .insert(app.artists, {
                  organizationId: org.id,
                  name: "Blue Hour",
                  genre: "jazz",
                  status: "active",
                })
                .wait({ tier: "edge" });
              // A third row proves the subscriber keeps the query's actual
              // ordered window rather than merely eventually receiving all
              // authorized artists after reconnect.
              await writer
                .insert(app.artists, {
                  organizationId: org.id,
                  name: "Zither",
                  genre: "folk",
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
              // A second authorized release is a planted positive for the
              // query's bound below. If lowering drops `limit(1)`, the edge
              // and reopened-local assertions must expose it instead of
              // merely proving eventual include delivery.
              await writer
                .insert(app.releases, {
                  organizationId: org.id,
                  artistId: artist.id,
                  title: "Second Light",
                  releaseDate: new Date("2027-01-01"),
                  status: "scheduled",
                })
                .wait({ tier: "edge" });
            },
          },
          {
            name: "offline editor retains its bounded local roster",
            async run() {
              const localRows = await reader.all(roster, { tier: "local" });
              expect(localRows.map((artist) => artist.name)).toContain("Aster");
              expect(localRows).toHaveLength(1);
            },
            faultsAfter: [{ kind: "reconnect", target: "browser-edge" }],
          },
          {
            name: "editor subscription and bounded release include converge after reconnect",
            async run() {
              await waitForCondition(
                async () =>
                  snapshots.some(
                    (rows) => rows.length === 2 && rows[0] === "Aster" && rows[1] === "Blue Hour",
                  ),
                15_000,
                "reader did not retain the ordered bounded BigLabel roster after reconnect",
              );
              await expect(reader.all(releasePipeline, { tier: "edge" })).resolves.toMatchObject([
                {
                  title: "First Light",
                  organizationId: org.id,
                  artist: { name: "Blue Hour", organizationId: org.id },
                },
              ]);
              await expect(reader.all(releasePipeline, { tier: "edge" })).resolves.toHaveLength(1);
            },
          },
          {
            name: "editor reopens persistent cache without refetching bounded app queries",
            async run() {
              ctx.untrack(reader);
              await reader.close();
              reader = await openDb({
                appId,
                serverUrl,
                jwtToken: editorToken,
                label: "big-label-reader",
              });
              await expect(reader.all(roster, { tier: "local" })).resolves.toMatchObject([
                { name: "Aster", organizationId: org.id },
                { name: "Blue Hour", organizationId: org.id },
              ]);
              await expect(reader.all(roster, { tier: "local" })).resolves.toHaveLength(2);
              await expect(reader.all(releasePipeline, { tier: "local" })).resolves.toMatchObject([
                {
                  title: "First Light",
                  organizationId: org.id,
                  artist: { name: "Blue Hour", organizationId: org.id },
                },
              ]);
              await expect(reader.all(releasePipeline, { tier: "local" })).resolves.toHaveLength(1);
            },
          },
          {
            name: "unaffiliated external edge cannot read either tenant query shape",
            async run() {
              const outsiderToken = await getJazzServerJwtForUser("outsider", {}, appId);
              const outsider = await openDb({
                appId,
                serverUrl,
                jwtToken: outsiderToken,
                label: "big-label-outsider",
              });
              await expect(outsider.all(roster, { tier: "edge" })).resolves.toEqual([]);
              await expect(outsider.all(releasePipeline, { tier: "edge" })).resolves.toEqual([]);
            },
          },
          {
            name: "revoking editor membership removes bounded tenant queries and includes",
            async run() {
              const editorMembership = await writer.all(
                app.memberships.where({ organizationId: org.id, userId: "editor" }).limit(1),
                { tier: "edge" },
              );
              expect(editorMembership).toHaveLength(1);
              await writer.delete(app.memberships, editorMembership[0]!.id).wait({ tier: "edge" });
              await waitForCondition(
                async () => {
                  const [artists, releases] = await Promise.all([
                    reader.all(roster, { tier: "edge" }),
                    reader.all(releasePipeline, { tier: "edge" }),
                  ]);
                  return artists.length === 0 && releases.length === 0;
                },
                15_000,
                "revoked editor retained a BigLabel tenant query or included artist",
              );
            },
          },
        ],
      },
      browserTopologyReporter,
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
