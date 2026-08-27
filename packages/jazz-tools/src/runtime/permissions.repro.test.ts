import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import type { RowRefValue } from "../permissions/index.js";
import { canonicalAuthorSubject } from "./author-id.js";
import { deploy } from "../dev/catalogue.js";
import { startLocalJazzServer } from "../testing/index.js";

const reproApp = s.defineApp({
  teams: s.table({
    name: s.string(),
    route_key: s.string(),
    corporation_id: s.string(),
    kind: s.string(),
    identity_key: s.string().optional(),
    system_owned: s.boolean(),
    archived: s.boolean(),
  }),
  user_team_edges: s.table({
    user_id: s.string(),
    team: s.ref("teams"),
    administrator: s.boolean(),
  }),
  team_team_edges: s.table({
    child_team: s.ref("teams"),
    parent_team: s.ref("teams"),
    administrator: s.boolean(),
  }),
  team_access_edges: s.table({
    target_team: s.ref("teams"),
    team: s.ref("teams"),
    grant_role: s.string(),
    administrator: s.boolean(),
  }),
});

const doubleRefReproApp = s.defineApp({
  teams: s.table({
    name: s.string(),
  }),
  team_entry: s.table({
    team_id: s.ref("teams"),
    target_id: s.ref("teams"),
    user_id: s.string(),
    administrator: s.boolean(),
  }),
  dropdowns: s.table({
    name: s.string(),
  }),
  dropdowns_access_edges: s.table({
    resource_id: s.ref("dropdowns"),
    team_id: s.ref("teams"),
    grant_role: s.string(),
    administrator: s.boolean(),
  }),
});

const relatedWriteApp = s.defineApp({
  playlists: s.table({ name: s.string() }),
  invitations: s.table({
    playlist_id: s.ref("playlists"),
    subject: s.string(),
    role: s.enum("listener", "editor"),
    status: s.enum("pending", "accepted", "revoked"),
  }),
  playlist_entries: s.table({
    playlist_id: s.ref("playlists"),
    position: s.int(),
  }),
});

const relatedWritePermissions = s.definePermissions(
  relatedWriteApp,
  ({ policy, anyOf, session, allowedTo }) => {
    const canReadPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.user },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          subject: session.user,
          status: "accepted",
        }),
      ]);
    const hasEditorInvitation = (playlistId: RowRefValue) =>
      policy.invitations.exists.where({
        playlist_id: playlistId,
        subject: session.user,
        role: "editor",
        status: "accepted",
      });
    policy.playlists.allowRead.where((playlist) => canReadPlaylist(playlist.id));
    policy.playlists.allowInsert.always();
    policy.playlists.allowUpdate.where({ $createdBy: session.user });
    policy.invitations.allowInsert.always();
    policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
    policy.playlist_entries.allowInsert.where((entry) =>
      anyOf([allowedTo.update("playlist_id"), hasEditorInvitation(entry.playlist_id)]),
    );
  },
);

type ReproPermissions = Parameters<typeof definePermissions<typeof reproApp>>[1];

const REPRO_ISSUER = "https://issuer.example";
const reproUser = (subject: string) => canonicalAuthorSubject(REPRO_ISSUER, subject);

function seedScenario(context: JazzContext): void {
  const db = context.db(reproApp);

  const { value: directTeam } = db.insert(reproApp.teams, {
    name: "Direct Membership",
    route_key: "base-direct",
    corporation_id: "corp",
    kind: "individual",
    identity_key: reproUser("alice"),
    system_owned: false,
    archived: false,
  });
  const { value: relationTeam } = db.insert(reproApp.teams, {
    name: "Relation Membership",
    route_key: "relation-direct",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const { value: qualifiedTeam } = db.insert(reproApp.teams, {
    name: "Qualified Predicate",
    route_key: "qualified-predicate",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const { value: opsTeam } = db.insert(reproApp.teams, {
    name: "Operations",
    route_key: "gather-target",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const { value: grantTargetTeam } = db.insert(reproApp.teams, {
    name: "Incident Desk",
    route_key: "grant-target",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  db.insert(reproApp.teams, {
    name: "Hidden Team",
    route_key: "hidden",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });

  db.insert(reproApp.user_team_edges, {
    user_id: reproUser("alice"),
    team: directTeam.id,
    administrator: false,
  });
  db.insert(reproApp.user_team_edges, {
    user_id: reproUser("alice"),
    team: relationTeam.id,
    administrator: false,
  });
  db.insert(reproApp.user_team_edges, {
    user_id: reproUser("alice"),
    team: qualifiedTeam.id,
    administrator: false,
  });
  db.insert(reproApp.team_team_edges, {
    child_team: directTeam.id,
    parent_team: opsTeam.id,
    administrator: false,
  });
  db.insert(reproApp.team_access_edges, {
    target_team: grantTargetTeam.id,
    team: relationTeam.id,
    grant_role: "viewer",
    administrator: false,
  });
}

function sortNames(rows: Array<{ name: string }>): string[] {
  return rows.map((row) => row.name).sort();
}

function sortGrantRoles(rows: Array<{ grant_role: string }>): string[] {
  return rows.map((row) => row.grant_role).sort();
}

type JazzContext = import("../backend/create-jazz-context.js").JazzContext;

async function createReproContext(defineCasePermissions: ReproPermissions): Promise<JazzContext> {
  const appId = randomUUID();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-permissions-repro-"));
  const dataPath = join(dataRoot, "runtime.db");

  const permissions = definePermissions(reproApp, defineCasePermissions);
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId,
    app: reproApp,
    permissions,
    driver: { type: "persistent", dataPath },
    env: "test",
    tier: "edge",
  });
  onTestFinished(async () => {
    context.flush();
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });

  return context;
}

async function createServerBackedReproContext(
  defineCasePermissions: ReproPermissions,
  tier: "local" | "edge" | "global" = "edge",
): Promise<JazzContext> {
  const appId = randomUUID();
  const backendSecret = `permissions-repro-backend-${appId}`;
  const adminSecret = `permissions-repro-admin-${appId}`;
  const server = await startLocalJazzServer({
    appId,
    backendSecret,
    adminSecret,
  });
  const permissions = definePermissions(reproApp, defineCasePermissions);
  await deploy({
    appId,
    serverUrl: server.url,
    adminSecret,
    schema: reproApp,
    permissions,
  });

  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId: server.appId,
    app: reproApp,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    tier,
  });

  onTestFinished(async () => {
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await server.stop();
  });

  return context;
}

describe("runtime permission repros for recursive gather and qualified predicates", () => {
  it("keeps reader child inserts denied while editor child inserts settle at Edge", async () => {
    const appId = randomUUID();
    const backendSecret = `related-write-repro-backend-${appId}`;
    const adminSecret = `related-write-repro-admin-${appId}`;
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    let context: JazzContext | null = null;

    try {
      await deploy({
        appId,
        serverUrl: server.url,
        adminSecret,
        schema: relatedWriteApp,
        permissions: relatedWritePermissions,
      });
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: server.appId,
        app: relatedWriteApp,
        permissions: relatedWritePermissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        tier: "edge",
      });
      const backend = context.asBackend(relatedWriteApp);
      const playlist = await backend
        .insert(relatedWriteApp.playlists, { name: "access" })
        .wait({ tier: "edge" });
      await backend
        .insert(relatedWriteApp.invitations, {
          playlist_id: playlist.id,
          subject: reproUser("reader"),
          role: "listener",
          status: "accepted",
        })
        .wait({ tier: "edge" });
      await backend
        .insert(relatedWriteApp.invitations, {
          playlist_id: playlist.id,
          subject: reproUser("editor"),
          role: "editor",
          status: "accepted",
        })
        .wait({ tier: "edge" });

      const reader = context.forSession(
        {
          issuer: REPRO_ISSUER,
          user_id: "reader",
          claims: {},
          authMode: "external",
        },
        relatedWriteApp,
      );
      const editor = context.forSession(
        {
          issuer: REPRO_ISSUER,
          user_id: "editor",
          claims: {},
          authMode: "external",
        },
        relatedWriteApp,
      );
      await expect(
        reader
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 1 })
          .wait({ tier: "edge" }),
      ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
      await expect(
        reader
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 2 })
          .wait({ tier: "edge" }),
      ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
      expect(await reader.all(relatedWriteApp.playlist_entries.where({}))).toEqual([]);
      expect(await backend.all(relatedWriteApp.playlist_entries.where({}))).toEqual([]);
      await expect(
        editor
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 3 })
          .wait({ tier: "edge" }),
      ).resolves.toMatchObject({ position: 3 });
    } finally {
      await context?.shutdown();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await server.stop();
    }
  }, 30_000);

  it('keeps `allowedTo.read("target_team")` readable for team access rows', async () => {
    const context = await createServerBackedReproContext(
      ({ policy, allowedTo, anyOf, session }) => {
        const anyGrantRoleValues = ["viewer", "editor", "manager"];
        const teamIds = session.claims["team_ids"];
        const adminTeamIds = session.claims["admin_team_ids"];
        const readableNonAdminTeamGrant = {
          team: { in: teamIds },
          grant_role: { in: anyGrantRoleValues },
          administrator: false,
        };
        const readableAdminTeamGrant = {
          team: { in: adminTeamIds },
          grant_role: { in: anyGrantRoleValues },
          administrator: true,
        };

        return [
          policy.teams.allowRead.where((team) =>
            anyOf([
              { identity_key: session.user },
              policy.team_access_edges.exists.where({
                target_team: team.id,
                ...readableNonAdminTeamGrant,
              }),
              policy.team_access_edges.exists.where({
                target_team: team.id,
                ...readableAdminTeamGrant,
              }),
            ]),
          ),
          policy.team_access_edges.allowRead.where(allowedTo.read("target_team", { maxDepth: 32 })),
        ];
      },
      "edge",
    );

    const db = context.asBackend(reproApp);
    const bobTeam = await db
      .insert(reproApp.teams, {
        name: "Bob",
        route_key: "bob",
        corporation_id: "corp",
        kind: "individual",
        identity_key: reproUser("bob"),
        system_owned: false,
        archived: false,
      })
      .wait({ tier: "edge" });
    await db
      .insert(reproApp.team_access_edges, {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "viewer",
        administrator: false,
      })
      .wait({ tier: "edge" });
    await db
      .insert(reproApp.team_access_edges, {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "manager",
        administrator: true,
      })
      .wait({ tier: "edge" });

    const bobDb = context.forSession(
      {
        user_id: reproUser("bob"),
        claims: {
          team_ids: [bobTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
      reproApp,
    );

    const teams = await bobDb.all(reproApp.teams.where({}));
    const grants = await bobDb.all(reproApp.team_access_edges.where({}));

    expect(sortNames(teams)).toEqual(["Bob"]);
    expect(sortGrantRoles(grants)).toEqual(["manager", "viewer"]);
  });

  it.fails("supports the full alpha.33 grant-closure repro end to end", async () => {
    const context = await createReproContext(({ policy, session, allOf }) => {
      const reachableTeams = policy.teams.gather({
        start: {
          "user_team_edges.user_id": session.user,
        },
        step: ({ current }) =>
          policy.team_team_edges
            .where({
              child_team: current,
              administrator: false,
            })
            .hopTo("parent_team"),
        maxDepth: 8,
      });

      return [
        policy.teams.allowRead.where((team) =>
          allOf([
            { route_key: "base-direct" },
            policy.user_team_edges.exists.where({
              user_id: session.user,
              team: team.id,
            }),
          ]),
        ),
        policy.teams.allowRead.where((team) =>
          allOf([
            { route_key: "relation-direct" },
            policy.exists(
              policy.user_team_edges.where({ user_id: session.user }).hopTo("team").where({
                id: team.id,
              }),
            ),
          ]),
        ),
        policy.teams.allowRead.where({
          route_key: "qualified-predicate",
          "user_team_edges.user_id": session.user,
        }),
        policy.teams.allowRead.where((team) =>
          allOf([
            { route_key: "gather-target" },
            policy.exists(
              reachableTeams.where({
                id: team.id,
              }),
            ),
          ]),
        ),
        policy.teams.allowRead.where((team) =>
          allOf([
            { route_key: "grant-target" },
            policy.exists(
              reachableTeams.hopTo("team_access_edgesViaTeam").where({
                "team_access_edges.target_team": team.id,
                grant_role: { in: ["viewer", "editor", "manager"] },
                administrator: false,
              }),
            ),
          ]),
        ),
      ];
    });

    seedScenario(context);

    const aliceDb = context.forSession(
      {
        user_id: "alice",
        claims: {},
        issuer: "https://issuer.example",
        authMode: "external",
      },
      reproApp,
    );

    const names = (await aliceDb.all(reproApp.teams.where({}))).map((team) => team.name).sort();
    expect(names).toEqual(
      [
        "Direct Membership",
        "Incident Desk",
        "Operations",
        "Qualified Predicate",
        "Relation Membership",
      ].sort(),
    );
  });

  it("evaluates explicit gather seeds through a same-table double-ref edge", async () => {
    const appId = randomUUID();
    const userTeamId = randomUUID();
    const dataRoot = await mkdtemp(join(tmpdir(), "jazz-double-ref-permissions-repro-"));
    const dataPath = join(dataRoot, "runtime.db");
    const permissions = definePermissions(doubleRefReproApp, ({ policy, session }) => {
      const directTeams = policy.team_entry.where({ user_id: session.user }).hopTo("target");
      const reachableTeams = policy.teams.gather({
        start: directTeams,
        step: ({ current }) =>
          policy.team_entry.where({ team_id: current, administrator: false }).hopTo("target"),
        maxDepth: 8,
      });

      return [
        policy.dropdowns.allowRead.where((dropdown) =>
          policy.exists(
            reachableTeams.hopTo("dropdowns_access_edgesViaTeam").where({
              "dropdowns_access_edges.resource_id": dropdown.id,
              grant_role: { in: ["viewer"] },
              administrator: false,
            }),
          ),
        ),
      ];
    });
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const context = createJazzContext({
      appId,
      app: doubleRefReproApp,
      permissions,
      driver: { type: "persistent", dataPath },
      env: "test",
      tier: "edge",
    });
    onTestFinished(async () => {
      context.flush();
      await context.shutdown();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await rm(dataRoot, { recursive: true, force: true });
    });

    const db = context.db(doubleRefReproApp);
    const { value: userTeam } = db.insert(
      doubleRefReproApp.teams,
      { name: "User" },
      { id: userTeamId },
    );
    const { value: directTeam } = db.insert(doubleRefReproApp.teams, { name: "Direct" });
    const { value: nestedTeam } = db.insert(doubleRefReproApp.teams, { name: "Nested" });
    const { value: dropdown } = db.insert(doubleRefReproApp.dropdowns, { name: "Visible" });
    db.insert(doubleRefReproApp.team_entry, {
      team_id: userTeam.id,
      target_id: directTeam.id,
      user_id: reproUser(userTeam.id),
      administrator: false,
    });
    db.insert(doubleRefReproApp.team_entry, {
      team_id: directTeam.id,
      target_id: nestedTeam.id,
      user_id: reproUser("unused"),
      administrator: false,
    });
    db.insert(doubleRefReproApp.dropdowns_access_edges, {
      resource_id: dropdown.id,
      team_id: nestedTeam.id,
      grant_role: "viewer",
      administrator: false,
    });

    const userDb = context.forSession(
      {
        user_id: userTeam.id,
        claims: {},
        issuer: "https://issuer.example",
        authMode: "external",
      },
      doubleRefReproApp,
    );

    await expect(userDb.all(doubleRefReproApp.dropdowns.where({}))).resolves.toEqual([
      expect.objectContaining({ id: dropdown.id }),
    ]);
  });

  it("keeps shared-context session reads stable across session order", async () => {
    const context = await createServerBackedReproContext(
      ({ policy, allowedTo, anyOf, session }) => {
        const anyGrantRoleValues = ["viewer", "editor", "manager"];
        const teamIds = session.claims["team_ids"];
        const adminTeamIds = session.claims["admin_team_ids"];
        const readableNonAdminTeamGrant = {
          team: { in: teamIds },
          grant_role: { in: anyGrantRoleValues },
          administrator: false,
        };
        const readableAdminTeamGrant = {
          team: { in: adminTeamIds },
          grant_role: { in: anyGrantRoleValues },
          administrator: true,
        };

        return [
          policy.teams.allowRead.where((team) =>
            anyOf([
              { identity_key: session.user },
              policy.team_access_edges.exists.where({
                target_team: team.id,
                ...readableNonAdminTeamGrant,
              }),
              policy.team_access_edges.exists.where({
                target_team: team.id,
                ...readableAdminTeamGrant,
              }),
            ]),
          ),
          policy.team_access_edges.allowRead.where(allowedTo.read("target_team", { maxDepth: 32 })),
        ];
      },
      "local",
    );

    const db = context.asBackend(reproApp);
    const { value: aliceTeam } = db.insert(reproApp.teams, {
      name: "Alice",
      route_key: "alice",
      corporation_id: "corp",
      kind: "individual",
      identity_key: reproUser("alice"),
      system_owned: false,
      archived: false,
    });
    const { value: bobTeam } = db.insert(reproApp.teams, {
      name: "Bob",
      route_key: "bob",
      corporation_id: "corp",
      kind: "individual",
      identity_key: reproUser("bob"),
      system_owned: false,
      archived: false,
    });
    const { value: internTeam } = db.insert(reproApp.teams, {
      name: "Intern",
      route_key: "intern",
      corporation_id: "corp",
      kind: "individual",
      identity_key: reproUser("intern"),
      system_owned: false,
      archived: false,
    });
    const { value: opsTeam } = db.insert(reproApp.teams, {
      name: "Ops",
      route_key: "ops",
      corporation_id: "corp",
      kind: "manual",
      system_owned: false,
      archived: false,
    });
    const { value: regionalTeam } = db.insert(reproApp.teams, {
      name: "Regional",
      route_key: "regional",
      corporation_id: "corp",
      kind: "manual",
      system_owned: false,
      archived: false,
    });

    for (const teamId of [aliceTeam.id, bobTeam.id, internTeam.id, opsTeam.id, regionalTeam.id]) {
      db.insert(reproApp.team_access_edges, {
        target_team: teamId,
        team: teamId,
        grant_role: "viewer",
        administrator: false,
      });
      db.insert(reproApp.team_access_edges, {
        target_team: teamId,
        team: teamId,
        grant_role: "manager",
        administrator: true,
      });
    }

    db.insert(reproApp.team_access_edges, {
      target_team: opsTeam.id,
      team: aliceTeam.id,
      grant_role: "manager",
      administrator: false,
    });
    db.insert(reproApp.team_access_edges, {
      target_team: regionalTeam.id,
      team: opsTeam.id,
      grant_role: "editor",
      administrator: false,
    });
    db.insert(reproApp.team_access_edges, {
      target_team: internTeam.id,
      team: regionalTeam.id,
      grant_role: "viewer",
      administrator: false,
    });

    const sessions = {
      alice: {
        user_id: "alice",
        claims: {
          team_ids: [aliceTeam.id, opsTeam.id, regionalTeam.id, internTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
      bob: {
        user_id: "bob",
        claims: {
          team_ids: [bobTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
      intern: {
        user_id: "intern",
        claims: {
          team_ids: [internTeam.id, regionalTeam.id, aliceTeam.id, opsTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
    } as const;

    const expectedNames = {
      alice: ["Alice", "Intern", "Ops", "Regional"],
      bob: ["Bob"],
      intern: ["Alice", "Intern", "Ops", "Regional"],
    } as const;

    const orders = [
      ["bob"],
      ["bob", "alice"],
      ["alice", "bob"],
      ["intern", "bob"],
      ["alice", "intern", "bob"],
    ] as const;

    for (const order of orders) {
      for (const actor of order) {
        const actorDb = context.forSession(sessions[actor], reproApp);
        expect(sortNames(await actorDb.all(reproApp.teams.where({})))).toEqual(
          expectedNames[actor],
        );
      }
    }
  });
});
