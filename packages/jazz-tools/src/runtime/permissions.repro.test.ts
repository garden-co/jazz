import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { localFirstAccountId } from "../accounts/local-first.js";
import { deploy } from "../dev/catalogue.js";
import {
  startLocalJazzServer,
  testApp,
  testPermissions,
  type TestPermissionsFile,
} from "../testing/index.js";

const reproApp = testApp({
  tables: {
    team_access_edges: {
      columns: {
        target_team: "uuid",
        team: "uuid",
        grant_role: "string",
        administrator: "boolean",
      },
      relations: {
        target_teamRelation: { rel: "teams", column: "target_team" },
        teamRelation: { rel: "teams", column: "team" },
      },
    },
    team_team_edges: {
      columns: { child_team: "uuid", parent_team: "uuid", administrator: "boolean" },
      relations: {
        child_teamRelation: { rel: "teams", column: "child_team" },
        parent_teamRelation: { rel: "teams", column: "parent_team" },
      },
    },
    teams: {
      columns: {
        name: "string",
        route_key: "string",
        corporation_id: "string",
        kind: "string",
        identity_key: "uuid?",
        system_owned: "boolean",
        archived: "boolean",
      },
      relations: {
        team_access_edgesViaTarget_team: {
          reverse: "team_access_edges",
          relation: "target_teamRelation",
        },
        team_access_edgesViaTeam: { reverse: "team_access_edges", relation: "teamRelation" },
        team_team_edgesViaChild_team: {
          reverse: "team_team_edges",
          relation: "child_teamRelation",
        },
        team_team_edgesViaParent_team: {
          reverse: "team_team_edges",
          relation: "parent_teamRelation",
        },
        user_team_edgesViaTeam: { reverse: "user_team_edges", relation: "teamRelation" },
      },
    },
    user_team_edges: {
      columns: { user_id: "uuid", team: "uuid", administrator: "boolean" },
      relations: { teamRelation: { rel: "teams", column: "team" } },
    },
  },
});

const doubleRefReproApp = testApp({
  tables: {
    dropdowns: {
      columns: { name: "string" },
      relations: {
        dropdowns_access_edgesViaResource: {
          reverse: "dropdowns_access_edges",
          relation: "resource",
        },
      },
    },
    dropdowns_access_edges: {
      columns: {
        resource_id: "uuid",
        team_id: "uuid",
        grant_role: "string",
        administrator: "boolean",
      },
      relations: {
        resource: { rel: "dropdowns", column: "resource_id" },
        teamRelation: { rel: "teams", column: "team_id" },
      },
    },
    team_entry: {
      columns: {
        team_id: "uuid",
        target_id: "uuid",
        user_id: "uuid",
        administrator: "boolean",
      },
      relations: {
        target: { rel: "teams", column: "target_id" },
        teamRelation: { rel: "teams", column: "team_id" },
      },
    },
    teams: {
      columns: { name: "string" },
      relations: {
        dropdowns_access_edgesViaTeam: {
          reverse: "dropdowns_access_edges",
          relation: "teamRelation",
        },
        team_entryViaTarget: { reverse: "team_entry", relation: "target" },
        team_entryViaTeam: { reverse: "team_entry", relation: "teamRelation" },
      },
    },
  },
});

const relatedWriteApp = testApp({
  tables: {
    invitations: {
      columns: {
        playlist_id: "uuid",
        subject: "uuid",
        role: { enum: ["listener", "editor"] },
        status: { enum: ["pending", "accepted", "revoked"] },
      },
      relations: { playlist: { rel: "playlists", column: "playlist_id" } },
    },
    playlist_entries: {
      columns: { playlist_id: "uuid", position: "int" },
      relations: { playlist: { rel: "playlists", column: "playlist_id" } },
    },
    playlists: {
      columns: { name: "string" },
      relations: {
        invitationsViaPlaylist: { reverse: "invitations", relation: "playlist" },
        playlist_entriesViaPlaylist: { reverse: "playlist_entries", relation: "playlist" },
      },
    },
  },
});

const relatedWritePermissionsFile = {
  format: 1,
  tables: {
    invitations: { insert: "true" },
    playlist_entries: {
      read: "allowedTo.read(playlist)",
      insert: {
        anyOf: [
          "allowedTo.update(playlist)",
          "exists invitations(playlist_id = this.playlist_id, subject = session.user, role = 'editor', status = 'accepted')",
        ],
      },
    },
    playlists: {
      read: {
        anyOf: [
          "$createdBy = session.user",
          "exists invitations(playlist_id = this.id, subject = session.user, status = 'accepted')",
        ],
      },
      insert: "true",
      update: "$createdBy = session.user",
    },
  },
};
const relatedWritePermissions = testPermissions(relatedWriteApp, relatedWritePermissionsFile);

// Teams readable by identity or by a team grant listed in the session's
// claims; their access rows readable through the target team.
const teamClaimsPermissions: TestPermissionsFile = {
  tables: {
    team_access_edges: { read: "allowedTo.read(target_teamRelation, maxDepth: 32)" },
    teams: {
      read: {
        anyOf: [
          "identity_key = session.user",
          "exists team_access_edges(target_team = this.id, team in session.claims.team_ids, grant_role in ['viewer', 'editor', 'manager'], administrator = false)",
          "exists team_access_edges(target_team = this.id, team in session.claims.admin_team_ids, grant_role in ['viewer', 'editor', 'manager'], administrator = true)",
        ],
      },
    },
  },
};

const REPRO_ISSUER = "https://issuer.example";
const reproUser = (subject: string) => localFirstAccountId("permission-repro", subject);

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

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- testApp rows are loosely typed
function sortNames(rows: Array<{ [column: string]: any }>): string[] {
  return rows.map((row) => row.name).sort();
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- testApp rows are loosely typed
function sortGrantRoles(rows: Array<{ [column: string]: any }>): string[] {
  return rows.map((row) => row.grant_role).sort();
}

type JazzContext = import("../backend/create-jazz-context.js").JazzContext;

// The alpha.33 grant-closure repro: the teams reachable from the user's own
// teams along non-admin parent edges (at most 8 hops), from `seed`.
const joinSeed =
  "from teams join user_team_edges as __join_0 on teams.id = __join_0.team " +
  "where __join_0.user_id = session.user";
const hopSeed =
  "from user_team_edges join teams as __hop_0 on user_team_edges.team = __hop_0.id " +
  "where user_team_edges.user_id = session.user select __hop_0.id as id";
const reachableTeamsFrom = (seed: string) =>
  `from recursive(seed: ${seed}, ` +
  "step: from team_team_edges where team_team_edges.administrator = bool(false) " +
  "and team_team_edges.child_team = id(frontier) select team_team_edges.parent_team as id, " +
  "frontier: id(row), dedupe: (id(row)), maxDepth: 8)";

/** The repro's five read rules, one per route_key, its gather seeded with `seed`. */
function grantClosurePermissions(seed: string): TestPermissionsFile {
  const reachableTeams = reachableTeamsFrom(seed);
  return {
    tables: {
      teams: {
        read: {
          anyOf: [
            {
              allOf: [
                "route_key = 'base-direct'",
                "exists user_team_edges(user_id = session.user, team = this.id)",
              ],
            },
            {
              allOf: [
                "route_key = 'relation-direct'",
                "exists user_team_edges(user_id = session.user).teamRelation(id = this.id)",
              ],
            },
            "exists (from teams join user_team_edges as __join_0 on teams.id = __join_0.team where teams.route_key = text('qualified-predicate') and __join_0.user_id = session.user where teams.id = id(outer))",
            {
              allOf: [
                "route_key = 'gather-target'",
                `exists (${reachableTeams} where teams.id = outer.id)`,
              ],
            },
            {
              allOf: [
                "route_key = 'grant-target'",
                `exists (${reachableTeams} join team_access_edges as __recursive_join_0 on teams.id = __recursive_join_0.team ` +
                  "where __recursive_join_0.target_team = outer.id " +
                  "and __recursive_join_0.grant_role in [text('viewer'), text('editor'), text('manager')] " +
                  "and __recursive_join_0.administrator = bool(false) select __recursive_join_0.id as id)",
              ],
            },
          ],
        },
      },
    },
  };
}

/** Every team seedScenario gives alice a way to read; only "Hidden Team" is left out. */
const ALL_ALICE_TEAMS = [
  "Direct Membership",
  "Incident Desk",
  "Operations",
  "Qualified Predicate",
  "Relation Membership",
];

async function aliceTeamNames(context: JazzContext): Promise<string[]> {
  const aliceDb = context.forSession(
    {
      user_id: "alice",
      account_id: reproUser("alice"),
      claims: {},
      issuer: REPRO_ISSUER,
      authMode: "external",
    },
    reproApp,
  );
  return sortNames(await aliceDb.all(reproApp.teams.where({})));
}

async function createReproContext(permissionsFile: TestPermissionsFile): Promise<JazzContext> {
  const appId = randomUUID();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-permissions-repro-"));
  const dataPath = join(dataRoot, "runtime.db");

  const permissions = testPermissions(reproApp, permissionsFile);
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId,
    app: reproApp,
    permissions,
    driver: { type: "persistent", dataPath },
    env: "test",
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
  permissionsFile: TestPermissionsFile,
): Promise<JazzContext> {
  const appId = randomUUID();
  const backendSecret = `permissions-repro-backend-${appId}`;
  const adminSecret = `permissions-repro-admin-${appId}`;
  const server = await startLocalJazzServer({
    appId,
    backendSecret,
    adminSecret,
  });
  const permissions = testPermissions(reproApp, permissionsFile);
  const { bundle } = await deploy({
    appId,
    serverUrl: server.url,
    adminSecret,
    app: reproApp,
    permissions: { format: 1, ...permissionsFile },
  });

  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    bundle,
    appId: server.appId,
    app: reproApp,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
  });

  onTestFinished(async () => {
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await server.stop();
  });

  return context;
}

describe("runtime permission repros for recursive gather and qualified predicates", () => {
  it("keeps reader child inserts denied while editor child inserts settle at Global", async () => {
    const appId = randomUUID();
    const backendSecret = `related-write-repro-backend-${appId}`;
    const adminSecret = `related-write-repro-admin-${appId}`;
    const server = await startLocalJazzServer({ appId, backendSecret, adminSecret });
    let context: JazzContext | null = null;

    try {
      const { bundle } = await deploy({
        appId,
        serverUrl: server.url,
        adminSecret,
        app: relatedWriteApp,
        permissions: relatedWritePermissionsFile,
      });
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        bundle,
        appId: server.appId,
        app: relatedWriteApp,
        permissions: relatedWritePermissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        env: "test",
      });
      const backend = context.asBackend(relatedWriteApp);
      const playlist = await backend
        .insert(relatedWriteApp.playlists, { name: "access" })
        .wait({ tier: "global" });
      await backend
        .insert(relatedWriteApp.invitations, {
          playlist_id: playlist.id,
          subject: reproUser("reader"),
          role: "listener",
          status: "accepted",
        })
        .wait({ tier: "global" });
      await backend
        .insert(relatedWriteApp.invitations, {
          playlist_id: playlist.id,
          subject: reproUser("editor"),
          role: "editor",
          status: "accepted",
        })
        .wait({ tier: "global" });

      const reader = context.forSession(
        {
          issuer: REPRO_ISSUER,
          user_id: "reader",
          account_id: reproUser("reader"),
          claims: {},
          authMode: "external",
        },
        relatedWriteApp,
      );
      const editor = context.forSession(
        {
          issuer: REPRO_ISSUER,
          user_id: "editor",
          account_id: reproUser("editor"),
          claims: {},
          authMode: "external",
        },
        relatedWriteApp,
      );
      await expect(
        reader
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 1 })
          .wait({ tier: "global" }),
      ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
      await expect(
        reader
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 2 })
          .wait({ tier: "global" }),
      ).rejects.toThrow(/AuthorizationDenied|Write rejected/);
      expect(await reader.all(relatedWriteApp.playlist_entries.where({}))).toEqual([]);
      expect(await backend.all(relatedWriteApp.playlist_entries.where({}))).toEqual([]);
      await expect(
        editor
          .insert(relatedWriteApp.playlist_entries, { playlist_id: playlist.id, position: 3 })
          .wait({ tier: "global" }),
      ).resolves.toMatchObject({ position: 3 });
    } finally {
      await context?.shutdown();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await server.stop();
    }
  }, 30_000);

  it('keeps `allowedTo.read("target_teamRelation")` readable for team access rows', async () => {
    const context = await createServerBackedReproContext(teamClaimsPermissions);

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
      .wait({ tier: "global" });
    await db
      .insert(reproApp.team_access_edges, {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "viewer",
        administrator: false,
      })
      .wait({ tier: "global" });
    await db
      .insert(reproApp.team_access_edges, {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "manager",
        administrator: true,
      })
      .wait({ tier: "global" });

    const bobDb = context.forSession(
      {
        user_id: "bob",
        account_id: reproUser("bob"),
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

  // A known-failing repro in .64 (`it.fails`): its gather seeds on a join
  // (teams ⋈ user_team_edges), which the engine refuses in policies. The
  // "grant-target" rule hops from the same gather and is refused the same way.
  it("refuses the alpha.33 grant-closure gather, whose seed is a join", () => {
    const reachableTeams =
      "from recursive(seed: from teams join user_team_edges as __join_0 on teams.id = __join_0.team " +
      "where __join_0.user_id = session.user, " +
      "step: from team_team_edges where team_team_edges.administrator = bool(false) " +
      "and team_team_edges.child_team = id(frontier) select team_team_edges.parent_team as id, " +
      "frontier: id(row), dedupe: (id(row)), maxDepth: 8)";
    const permissions = {
      tables: {
        teams: {
          read: {
            anyOf: [
              {
                allOf: [
                  "route_key = 'base-direct'",
                  "exists user_team_edges(user_id = session.user, team = this.id)",
                ],
              },
              {
                allOf: [
                  "route_key = 'relation-direct'",
                  "exists user_team_edges(user_id = session.user).teamRelation(id = this.id)",
                ],
              },
              "exists (from teams join user_team_edges as __join_0 on teams.id = __join_0.team where teams.route_key = text('qualified-predicate') and __join_0.user_id = session.user where teams.id = id(outer))",
              {
                allOf: [
                  "route_key = 'gather-target'",
                  `exists (${reachableTeams} where teams.id = outer.id)`,
                ],
              },
            ],
          },
        },
      },
    };

    expect(() => testPermissions(reproApp, permissions)).toThrow(
      "Gather seeds other than a filtered table scan are not supported in policies yet",
    );
  });

  // The target behaviour of the repro above, end to end: all five rules,
  // including "grant-target" (teams a reachable team holds a grant on), with
  // the gather seeded on the join. Known failing while the engine refuses
  // join seeds in policies ("not supported in policies yet"); when it starts
  // accepting them, this flips and should become a plain `it`.
  it.fails("supports the full alpha.33 grant-closure repro end to end", async () => {
    const context = await createReproContext(grantClosurePermissions(joinSeed));
    seedScenario(context);

    expect(await aliceTeamNames(context)).toEqual(ALL_ALICE_TEAMS);
  });

  // The same five rules with the seed written as the supported hop (from the
  // user's edges to their teams): the expected result is reachable, so the
  // `it.fails` above differs only by the seed's shape.
  it("supports the full alpha.33 grant-closure repro with a hop seed", async () => {
    const context = await createReproContext(grantClosurePermissions(hopSeed));
    seedScenario(context);

    expect(await aliceTeamNames(context)).toEqual(ALL_ALICE_TEAMS);
  });

  it("evaluates explicit gather seeds through a same-table double-ref edge", async () => {
    const appId = randomUUID();
    const userTeamId = randomUUID();
    const dataRoot = await mkdtemp(join(tmpdir(), "jazz-double-ref-permissions-repro-"));
    const dataPath = join(dataRoot, "runtime.db");
    const permissions = testPermissions(doubleRefReproApp, {
      tables: {
        dropdowns: {
          // Teams reachable from the user's own entries along `target`, then
          // their viewer edges to this dropdown.
          read:
            "exists (from recursive(seed: from team_entry join teams as __hop_0 on team_entry.target_id = __hop_0.id " +
            "where team_entry.user_id = session.user select __hop_0.id as id, " +
            "step: from team_entry where team_entry.administrator = bool(false) " +
            "and team_entry.team_id = id(frontier) select team_entry.target_id as id, " +
            "frontier: id(row), dedupe: (id(row)), maxDepth: 8) " +
            "join dropdowns_access_edges as __recursive_join_0 on teams.id = __recursive_join_0.team_id " +
            "where __recursive_join_0.resource_id = outer.id and __recursive_join_0.grant_role in [text('viewer')] " +
            "and __recursive_join_0.administrator = bool(false) select __recursive_join_0.id as id)",
        },
      },
    });
    const { createJazzContext } = await import("../backend/create-jazz-context.js");
    const context = createJazzContext({
      appId,
      app: doubleRefReproApp,
      permissions,
      driver: { type: "persistent", dataPath },
      env: "test",
    });
    onTestFinished(async () => {
      context.flush();
      await context.shutdown();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await rm(dataRoot, { recursive: true, force: true });
    });

    const db = context.db(doubleRefReproApp);
    const userTeam = await db
      .insert(doubleRefReproApp.teams, { name: "User" }, { id: userTeamId })
      .wait({ tier: "local" });
    const directTeam = await db
      .insert(doubleRefReproApp.teams, { name: "Direct" })
      .wait({ tier: "local" });
    const nestedTeam = await db
      .insert(doubleRefReproApp.teams, { name: "Nested" })
      .wait({ tier: "local" });
    const dropdown = await db
      .insert(doubleRefReproApp.dropdowns, { name: "Visible" })
      .wait({ tier: "local" });
    const sourceOnlyDropdown = await db
      .insert(doubleRefReproApp.dropdowns, { name: "Not reachable through target_id" })
      .wait({ tier: "local" });
    await db
      .insert(doubleRefReproApp.team_entry, {
        team_id: userTeam.id,
        target_id: directTeam.id,
        user_id: reproUser(userTeam.id),
        administrator: false,
      })
      .wait({ tier: "local" });
    await db
      .insert(doubleRefReproApp.team_entry, {
        team_id: directTeam.id,
        target_id: nestedTeam.id,
        user_id: reproUser("unused"),
        administrator: false,
      })
      .wait({ tier: "local" });
    await db
      .insert(doubleRefReproApp.dropdowns_access_edges, {
        resource_id: dropdown.id,
        team_id: nestedTeam.id,
        grant_role: "viewer",
        administrator: false,
      })
      .wait({ tier: "local" });
    await db
      .insert(doubleRefReproApp.dropdowns_access_edges, {
        resource_id: sourceOnlyDropdown.id,
        team_id: userTeam.id,
        grant_role: "viewer",
        administrator: false,
      })
      .wait({ tier: "local" });

    const userDb = context.forSession(
      {
        user_id: userTeam.id,
        account_id: reproUser(userTeam.id),
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
    const context = await createServerBackedReproContext(teamClaimsPermissions);

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
        account_id: reproUser("alice"),
        claims: {
          team_ids: [aliceTeam.id, opsTeam.id, regionalTeam.id, internTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
      bob: {
        user_id: "bob",
        account_id: reproUser("bob"),
        claims: {
          team_ids: [bobTeam.id],
          admin_team_ids: [],
        },
        issuer: REPRO_ISSUER,
        authMode: "external",
      },
      intern: {
        user_id: "intern",
        account_id: reproUser("intern"),
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
