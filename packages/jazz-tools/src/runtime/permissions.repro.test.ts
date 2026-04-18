import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzContext, type JazzContext } from "../backend/create-jazz-context.js";

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

type ReproEnv = {
  context: JazzContext;
  dataRoot: string;
};

function seedScenario(context: JazzContext): void {
  const db = context.db(reproApp);

  const aliceTeam = db.insert(reproApp.teams, {
    name: "Alice",
    route_key: "alice",
    corporation_id: "corp",
    kind: "individual",
    identity_key: "alice",
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

  db.insert(reproApp.user_team_edges, {
    user_id: "alice",
    team: aliceTeam.id,
    administrator: true,
  });
  db.insert(reproApp.team_team_edges, {
    child_team: aliceTeam.id,
    parent_team: opsTeam.id,
    administrator: false,
  });
  db.insert(reproApp.team_access_edges, {
    target_team: opsTeam.id,
    team: aliceTeam.id,
    grant_role: "viewer",
    administrator: false,
  });
}

async function runCase(
  expectedNames: string[],
  defineCasePermissions: ReproPermissions,
): Promise<string[]> {
  const appId = randomUUID();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-permissions-repro-"));
  const dataPath = join(dataRoot, "runtime.db");

  const permissions = definePermissions(reproApp, defineCasePermissions);
  const context = createJazzContext({
    appId,
    app: reproApp,
    permissions,
    driver: { type: "persistent", dataPath },
    env: "test",
    userBranch: "main",
    tier: "edge",
  });
  const env: ReproEnv = { context, dataRoot };

  try {
    seedScenario(context);

    const aliceDb = context.forSession(
      {
        user_id: "alice",
        claims: {},
      },
      reproApp,
    );

    const names = (await aliceDb.all(reproApp.teams.where({}))).map((team) => team.name).sort();
    expect(names).toEqual([...expectedNames].sort());
    return names;
  } finally {
    await env.context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(env.dataRoot, { recursive: true, force: true });
  }
}

describe("runtime permission repros for recursive gather and qualified predicates", () => {
  it("matches the original four runtime repro cases", async () => {
    await runCase(["Alice"], ({ policy, session }) => {
      policy.teams.allowRead.where((team) =>
        policy.user_team_edges.exists.where({
          user_id: session.user_id,
          team: team.id,
        }),
      );
    });

    await runCase(["Alice"], ({ policy, session }) => {
      const directTeams = policy.user_team_edges.where({ user_id: session.user_id }).hopTo("team");
      policy.teams.allowRead.where((team) =>
        policy.exists(
          directTeams.where({
            id: team.id,
          }),
        ),
      );
    });

    await runCase(["Alice"], ({ policy, session }) => {
      policy.teams.allowRead.where({
        "user_team_edges.user_id": session.user_id,
      });
    });

    await runCase(["Alice", "Ops"], ({ policy, session }) => {
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

      policy.teams.allowRead.where((team) =>
        policy.exists(
          reachableTeams.where({
            id: team.id,
          }),
        ),
      );
    });
  });

  it("supports correlated exists over a gathered team closure hopped through grants", async () => {
    await runCase(["Ops"], ({ policy, session }) => {
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

      policy.teams.allowRead.where((team) =>
        policy.exists(
          reachableTeams.hopTo("team_access_edgesViaTeam").where({
            "team_access_edges.target_team": team.id,
            grant_role: { in: ["viewer", "editor", "manager"] },
            administrator: false,
          }),
        ),
      );
    });
  });
});
