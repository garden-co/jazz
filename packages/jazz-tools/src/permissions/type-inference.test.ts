import { describe, expect, expectTypeOf, it } from "vitest";
import { col } from "../dsl.js";
import { defineApp, defineTable } from "../typed-app.js";
import { createSessionContext, definePermissions } from "./index.js";

const app = defineApp({
  projects: defineTable({
    ownerId: col.string(),
  }),
  branches: defineTable({
    projectId: col.ref("projects"),
    ownerId: col.string(),
    name: col.string(),
  }),
  todos: defineTable({
    ownerId: col.string(),
    done: col.boolean(),
    projectId: col.ref("projects").optional(),
  }),
  teams: defineTable({
    kind: col.string(),
    identity_key: col.string().optional(),
  }),
  team_team_edges: defineTable({
    child_team: col.ref("teams"),
    parent_team: col.ref("teams"),
  }),
  resource_access_edges: defineTable({
    team: col.ref("teams"),
    resource: col.ref("todos"),
    grant_role: col.string(),
  }),
});

describe("permissions type inference", () => {
  it("infers row callback and where key types", () => {
    definePermissions(app, ({ policy, anyOf, allowedTo, session, isCreator }) => {
      expectTypeOf(session.user_id.path).toEqualTypeOf<string[]>();
      expectTypeOf(session.userId.path).toEqualTypeOf<string[]>();
      expectTypeOf(session["claims.role"]!.path).toEqualTypeOf<string[]>();
      expectTypeOf(isCreator).toMatchTypeOf<Parameters<typeof anyOf>[0][number]>();

      const reachableTeams = policy.teams.gather({
        start: { kind: "individual", identity_key: session.userId },
        step: ({ current }) =>
          policy.team_team_edges.where({ child_team: current }).hopTo("parent_team"),
      });

      function hasViewerGrant(resource: unknown) {
        return policy.exists(
          reachableTeams.hopTo("resource_access_edgesViaTeam").where({
            "resource_access_edges.resource": resource,
            grant_role: "viewer",
          }),
        );
      }

      return [
        policy.todos.allowRead.where((todo) =>
          anyOf([
            { done: false },
            isCreator,
            session.where({ "claims.role": "manager" }),
            policy.projects.exists.where({
              id: todo.projectId,
              ownerId: session.userId,
            }),
            hasViewerGrant(todo.id),
          ]),
        ),
        policy.todos.allowUpdate
          .whereOld(allowedTo.update("projectId", { maxDepth: 4 }))
          .whereNew(allowedTo.update("projectId", { maxDepth: 4 })),
        policy.projects.allowRead.where(allowedTo.readReferencing(policy.todos, "projectId")),
        policy.teams.allowRead.where({
          "resource_access_edges.grant_role": "viewer",
        }),
      ];
    });
  });

  it("exposes never() on read/insert/update/delete builders", () => {
    definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.never(),
      policy.todos.allowInsert.never(),
      policy.todos.allowUpdate.never(),
      policy.todos.allowDelete.never(),
    ]);
  });

  it("exposes always() on read/insert/update/delete builders", () => {
    definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.always(),
      policy.todos.allowInsert.always(),
      policy.todos.allowUpdate.always(),
      policy.todos.allowDelete.always(),
    ]);
  });

  it("infers forBranch backing row and branch policy table types", () => {
    definePermissions(app, ({ policy, session }) => {
      policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
        expectTypeOf($branch.id).toMatchTypeOf<unknown>();
        expectTypeOf($branch.projectId).toMatchTypeOf<unknown>();

        branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
        branchPolicy.todos.allowInsert.where({
          projectId: $branch.projectId,
          ownerId: session.user_id,
        });

        if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
          // @ts-expect-error backing row exposes only known branch columns
          branchPolicy.todos.allowRead.where({ projectId: $branch.missingColumn });

          // @ts-expect-error branchPolicy exposes only app tables
          branchPolicy.missingTable.allowRead.where({});

          // @ts-expect-error invalid target table where key is still rejected
          branchPolicy.todos.allowRead.where({ missingColumn: $branch.projectId });
        }
      });
    });
  });

  it("rejects invalid table/column usage at compile time where possible", () => {
    definePermissions(app, ({ policy, allowedTo }) => [
      policy.todos.allowRead.where({ done: true }),
      policy.todos.allowRead.where(allowedTo.read("projectId")),
    ]);

    definePermissions(app, ({ policy }) => {
      // Type-level negative checks only: keep unreachable in normal runs.
      if ((globalThis as { __typecheck_only__?: boolean }).__typecheck_only__) {
        // @ts-expect-error unknown table key
        policy.unknown.allowRead.where({});

        // @ts-expect-error invalid where key for todos
        policy.todos.allowRead.where({ missingColumn: true });

        // @ts-expect-error invalid action name
        policy.todos.allowPublish.where({ done: true });

        // @ts-expect-error invalid exists where key for projects
        policy.projects.exists.where({ missingColumn: true });

        // @ts-expect-error row callback should expose only known todo columns
        policy.todos.allowRead.where((todo) => ({ ownerId: todo.missingColumn }));
      }

      return [];
    });
  });

  it("exposes managedByCreator() on table builders", () => {
    definePermissions(app, ({ policy }) => {
      policy.todos.managedByCreator();
      return [];
    });
  });
});

describe("SessionContext — typed authMode", () => {
  it("authMode compiles as a leaf Session field", () => {
    const session = createSessionContext();
    const condition = session.where({ authMode: "local-first" });
    expectTypeOf(condition).toMatchTypeOf<{ __jazzPermissionKind: "session-where" }>();
  });

  it("authMode accepts union of the three modes", () => {
    const session = createSessionContext();
    session.where({ authMode: "external" });
    session.where({ authMode: "anonymous" });
    session.where({ authMode: { in: ["local-first", "external"] } });
  });

  it("session.authMode produces a leaf SessionRefValue with path ['authMode']", () => {
    const session = createSessionContext();
    const ref = session.authMode;
    expect(ref).toMatchObject({
      __jazzPermissionKind: "session-ref",
      path: ["authMode"],
    });
  });
});
