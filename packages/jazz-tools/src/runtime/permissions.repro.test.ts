import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import { publishStoredSchema } from "./schema-fetch.js";
import { startLocalJazzServer } from "../testing/local-jazz-server.js";

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

type ReproPermissions = Parameters<typeof definePermissions<typeof reproApp>>[1];

function seedScenario(context: JazzContext): void {
  const db = context.db(reproApp);

  const directTeam = db.insert(reproApp.teams, {
    name: "Direct Membership",
    route_key: "base-direct",
    corporation_id: "corp",
    kind: "individual",
    identity_key: "alice",
    system_owned: false,
    archived: false,
  });
  const relationTeam = db.insert(reproApp.teams, {
    name: "Relation Membership",
    route_key: "relation-direct",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const qualifiedTeam = db.insert(reproApp.teams, {
    name: "Qualified Predicate",
    route_key: "qualified-predicate",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const opsTeam = db.insert(reproApp.teams, {
    name: "Operations",
    route_key: "gather-target",
    corporation_id: "corp",
    kind: "manual",
    system_owned: false,
    archived: false,
  });
  const grantTargetTeam = db.insert(reproApp.teams, {
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
    user_id: "alice",
    team: directTeam.id,
    administrator: false,
  });
  db.insert(reproApp.user_team_edges, {
    user_id: "alice",
    team: relationTeam.id,
    administrator: false,
  });
  db.insert(reproApp.user_team_edges, {
    user_id: "alice",
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
    userBranch: "main",
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
  await publishStoredSchema(server.url, {
    adminSecret,
    schema: reproApp.wasmSchema,
  });

  const permissions = definePermissions(reproApp, defineCasePermissions);
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId: server.appId,
    app: reproApp,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
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
  it('keeps `allowedTo.read("target_team")` readable for team access rows', async () => {
    const context = await createServerBackedReproContext(
      ({ policy, allowedTo, anyOf, session }) => {
        const anyGrantRoleValues = ["viewer", "editor", "manager"];
        const teamIds = session["claims.team_ids"];
        const adminTeamIds = session["claims.admin_team_ids"];
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
              { identity_key: session.user_id },
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
    const bobTeam = await db.insertDurable(
      reproApp.teams,
      {
        name: "Bob",
        route_key: "bob",
        corporation_id: "corp",
        kind: "individual",
        identity_key: "bob",
        system_owned: false,
        archived: false,
      },
      { tier: "edge" },
    );
    await db.insertDurable(
      reproApp.team_access_edges,
      {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "viewer",
        administrator: false,
      },
      { tier: "edge" },
    );
    await db.insertDurable(
      reproApp.team_access_edges,
      {
        target_team: bobTeam.id,
        team: bobTeam.id,
        grant_role: "manager",
        administrator: true,
      },
      { tier: "edge" },
    );

    const bobDb = context.forSession(
      {
        user_id: "bob",
        claims: {
          team_ids: [bobTeam.id],
          admin_team_ids: [],
        },
      },
      reproApp,
    );

    const teams = await bobDb.all(reproApp.teams.where({}));
    const grants = await bobDb.all(reproApp.team_access_edges.where({}));

    expect(sortNames(teams)).toEqual(["Bob"]);
    expect(sortGrantRoles(grants)).toEqual(["manager", "viewer"]);
  });

  it("supports the full alpha.33 grant-closure repro end to end", async () => {
    const context = await createReproContext(({ policy, session, allOf }) => {
      const reachableTeams = policy.teams.gather({
        start: {
          "user_team_edges.user_id": session.user_id,
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
              user_id: session.user_id,
              team: team.id,
            }),
          ]),
        ),
        policy.teams.allowRead.where((team) =>
          allOf([
            { route_key: "relation-direct" },
            policy.exists(
              policy.user_team_edges.where({ user_id: session.user_id }).hopTo("team").where({
                id: team.id,
              }),
            ),
          ]),
        ),
        policy.teams.allowRead.where({
          route_key: "qualified-predicate",
          "user_team_edges.user_id": session.user_id,
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

  it("keeps shared-context session reads stable across session order", async () => {
    const context = await createServerBackedReproContext(
      ({ policy, allowedTo, anyOf, session }) => {
        const anyGrantRoleValues = ["viewer", "editor", "manager"];
        const teamIds = session["claims.team_ids"];
        const adminTeamIds = session["claims.admin_team_ids"];
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
              { identity_key: session.user_id },
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
    const aliceTeam = db.insert(reproApp.teams, {
      name: "Alice",
      route_key: "alice",
      corporation_id: "corp",
      kind: "individual",
      identity_key: "alice",
      system_owned: false,
      archived: false,
    });
    const bobTeam = db.insert(reproApp.teams, {
      name: "Bob",
      route_key: "bob",
      corporation_id: "corp",
      kind: "individual",
      identity_key: "bob",
      system_owned: false,
      archived: false,
    });
    const internTeam = db.insert(reproApp.teams, {
      name: "Intern",
      route_key: "intern",
      corporation_id: "corp",
      kind: "individual",
      identity_key: "intern",
      system_owned: false,
      archived: false,
    });
    const opsTeam = db.insert(reproApp.teams, {
      name: "Ops",
      route_key: "ops",
      corporation_id: "corp",
      kind: "manual",
      system_owned: false,
      archived: false,
    });
    const regionalTeam = db.insert(reproApp.teams, {
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
      },
      bob: {
        user_id: "bob",
        claims: {
          team_ids: [bobTeam.id],
          admin_team_ids: [],
        },
      },
      intern: {
        user_id: "intern",
        claims: {
          team_ids: [internTeam.id, regionalTeam.id, aliceTeam.id, opsTeam.id],
          admin_team_ids: [],
        },
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
