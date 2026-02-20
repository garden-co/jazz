import { describe, expectTypeOf, it } from "vitest";
import { definePermissions } from "./index.js";

interface Todo {
  id: string;
  ownerId: string;
  done: boolean;
  projectId?: string;
}

interface TodoWhere {
  id?: string;
  ownerId?: string;
  done?: boolean;
  projectId?: string;
}

interface Project {
  id: string;
  ownerId: string;
}

interface ProjectWhere {
  id?: string;
  ownerId?: string;
}

interface Team {
  id: string;
  kind: string;
  identity_key?: string;
}

interface TeamWhere {
  id?: string;
  kind?: string;
  identity_key?: string;
}

interface TeamEdge {
  id: string;
  child_team: string;
  parent_team: string;
}

interface TeamEdgeWhere {
  id?: string;
  child_team?: string;
  parent_team?: string;
}

interface ResourceGrant {
  id: string;
  team: string;
  resource: string;
  grant_role: string;
}

interface ResourceGrantWhere {
  id?: string;
  team?: string;
  resource?: string;
  grant_role?: string;
}

class TodoQueryBuilder {
  declare readonly _rowType: Todo;
  where(_input: TodoWhere): TodoQueryBuilder {
    return this;
  }
}

class ProjectQueryBuilder {
  declare readonly _rowType: Project;
  where(_input: ProjectWhere): ProjectQueryBuilder {
    return this;
  }
}

class TeamQueryBuilder {
  declare readonly _rowType: Team;
  where(_input: TeamWhere): TeamQueryBuilder {
    return this;
  }
}

class TeamEdgeQueryBuilder {
  declare readonly _rowType: TeamEdge;
  where(_input: TeamEdgeWhere): TeamEdgeQueryBuilder {
    return this;
  }
}

class ResourceGrantQueryBuilder {
  declare readonly _rowType: ResourceGrant;
  where(_input: ResourceGrantWhere): ResourceGrantQueryBuilder {
    return this;
  }
}

const app = {
  todos: new TodoQueryBuilder(),
  projects: new ProjectQueryBuilder(),
  teams: new TeamQueryBuilder(),
  team_team_edges: new TeamEdgeQueryBuilder(),
  resource_access_edges: new ResourceGrantQueryBuilder(),
  wasmSchema: {
    tables: {
      todos: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "ownerId", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          {
            name: "projectId",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "projects",
          },
        ],
      },
      projects: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "ownerId", column_type: { type: "Text" }, nullable: false },
        ],
      },
      teams: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "kind", column_type: { type: "Text" }, nullable: false },
          { name: "identity_key", column_type: { type: "Text" }, nullable: true },
        ],
      },
      team_team_edges: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          {
            name: "child_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "parent_team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
        ],
      },
      resource_access_edges: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          {
            name: "team",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "teams",
          },
          {
            name: "resource",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "todos",
          },
          { name: "grant_role", column_type: { type: "Text" }, nullable: false },
        ],
      },
    },
  },
} as const;

describe("permissions type inference", () => {
  it("infers row callback and where key types", () => {
    definePermissions(app, ({ policy, anyOf, allowedTo, session }) => {
      expectTypeOf(session.userId.path).toEqualTypeOf<string[]>();

      const reachableTeams = policy.recursive({
        start: policy.teams
          .where({ kind: "individual", identity_key: session.userId })
          .select({ team: "id" }),
        step: ({ self }) =>
          self.join(policy.team_team_edges, { left: "team", right: "child_team" }).select({
            team: "parent_team",
          }),
      });

      const hasViewerGrant = (resource: unknown) =>
        policy.exists(
          reachableTeams.join(policy.resource_access_edges, { left: "team", right: "team" }).where({
            "resource_access_edges.resource": resource,
            grant_role: "viewer",
          }),
        );

      return [
        policy.todos.allowRead.where((todo) =>
          anyOf([
            { done: false },
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
      ];
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
});
