import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";

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

describe("runtime permission repros for recursive gather and qualified predicates", () => {
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
});
