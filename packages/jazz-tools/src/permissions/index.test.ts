import { describe, expect, it } from "vitest";
import { definePermissions } from "./index.js";
import type { PolicyExpr } from "../schema.js";

interface Todo {
  id: string;
  ownerId: string;
  archived: boolean;
  done: boolean;
  projectId?: string;
}

interface TodoWhere {
  id?: string;
  ownerId?: string;
  archived?: boolean;
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

interface TodoShare {
  id: string;
  todoId: string;
  userId: string;
  canRead: boolean;
}

interface TodoShareWhere {
  id?: string;
  todoId?: string;
  userId?: string;
  canRead?: boolean;
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

interface TeamTeamEdge {
  id: string;
  child_team: string;
  parent_team: string;
}

interface TeamTeamEdgeWhere {
  id?: string;
  child_team?: string;
  parent_team?: string;
}

interface ResourceAccessEdge {
  id: string;
  team: string;
  resource: string;
  grant_role: string;
}

interface ResourceAccessEdgeWhere {
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

class TodoShareQueryBuilder {
  declare readonly _rowType: TodoShare;
  where(_input: TodoShareWhere): TodoShareQueryBuilder {
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

class TeamTeamEdgeQueryBuilder {
  declare readonly _rowType: TeamTeamEdge;
  where(_input: TeamTeamEdgeWhere): TeamTeamEdgeQueryBuilder {
    return this;
  }
}

class ResourceAccessEdgeQueryBuilder {
  declare readonly _rowType: ResourceAccessEdge;
  where(_input: ResourceAccessEdgeWhere): ResourceAccessEdgeQueryBuilder {
    return this;
  }
}

const app = {
  todos: new TodoQueryBuilder(),
  projects: new ProjectQueryBuilder(),
  todoShares: new TodoShareQueryBuilder(),
  teams: new TeamQueryBuilder(),
  team_team_edges: new TeamTeamEdgeQueryBuilder(),
  resource_access_edges: new ResourceAccessEdgeQueryBuilder(),
  wasmSchema: {
    tables: {
      todos: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          { name: "ownerId", column_type: { type: "Text" }, nullable: false },
          { name: "archived", column_type: { type: "Boolean" }, nullable: false },
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
      todoShares: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          {
            name: "todoId",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "todos",
          },
          { name: "userId", column_type: { type: "Text" }, nullable: false },
          { name: "canRead", column_type: { type: "Boolean" }, nullable: false },
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
};

const appWithoutSchema = {
  todos: new TodoQueryBuilder(),
  projects: new ProjectQueryBuilder(),
  todoShares: new TodoShareQueryBuilder(),
  teams: new TeamQueryBuilder(),
  team_team_edges: new TeamTeamEdgeQueryBuilder(),
  resource_access_edges: new ResourceAccessEdgeQueryBuilder(),
};

describe("permissions DSL", () => {
  it("compiles read/insert/update/delete policies", () => {
    const compiled = definePermissions(app, ({ policy, allOf, allowedTo, session }) => [
      policy.todos.allowRead.where({ ownerId: session.userId }),
      policy.todos.allowInsert.where({ ownerId: session.userId }),
      policy.todos.allowUpdate
        .whereOld(allOf([allowedTo.update("projectId"), { archived: false }]))
        .whereNew(allowedTo.update("projectId")),
      policy.todos.allowDelete.where({ ownerId: session.userId }),
    ]);

    expect(compiled.todos.select?.using).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
    expect(compiled.todos.insert?.with_check).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
    expect(compiled.todos.update?.using).toEqual({
      type: "And",
      exprs: [
        {
          type: "Inherits",
          operation: "Update",
          via_column: "projectId",
        },
        {
          type: "Cmp",
          column: "archived",
          op: "Eq",
          value: {
            type: "Literal",
            value: false,
          },
        },
      ],
    });
    expect(compiled.todos.update?.with_check).toEqual({
      type: "Inherits",
      operation: "Update",
      via_column: "projectId",
    });
    expect(compiled.todos.delete?.using).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
  });

  it("supports plural action aliases and OR-merges repeated rules", () => {
    const compiled = definePermissions(app, ({ policy, anyOf, allowedTo, session }) => [
      policy.todos.allowReads.where({ ownerId: session.userId }),
      policy.todos.allowReads.where(anyOf([{ done: true }, allowedTo.read("projectId")])),
      policy.todos.allowInserts.where({ ownerId: session.userId }),
    ]);

    expect(compiled.todos.select?.using).toEqual({
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "ownerId",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["userId"],
          },
        },
        {
          type: "Cmp",
          column: "done",
          op: "Eq",
          value: {
            type: "Literal",
            value: true,
          },
        },
        {
          type: "Inherits",
          operation: "Select",
          via_column: "projectId",
        },
      ],
    });
    expect(compiled.todos.insert?.with_check).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
  });

  it("compiles non-correlated exists clauses", () => {
    const compiled = definePermissions(app, ({ policy, session }) => [
      policy.todos.allowRead.where(
        policy.todoShares.exists.where({
          userId: session.userId,
          canRead: true,
        }),
      ),
    ]);

    expect(compiled.todos.select?.using).toEqual({
      type: "Exists",
      table: "todoShares",
      condition: {
        type: "And",
        exprs: [
          {
            type: "Cmp",
            column: "userId",
            op: "Eq",
            value: {
              type: "SessionRef",
              path: ["userId"],
            },
          },
          {
            type: "Cmp",
            column: "canRead",
            op: "Eq",
            value: {
              type: "Literal",
              value: true,
            },
          },
        ],
      },
    });
  });

  it("supports allowedTo.insert and allowedTo.delete helpers", () => {
    const compiled = definePermissions(app, ({ policy, allowedTo }) => [
      policy.todos.allowInsert.where(allowedTo.insert("projectId")),
      policy.todos.allowDelete.where(allowedTo.delete("projectId")),
    ]);

    expect(compiled.todos.insert?.with_check).toEqual({
      type: "Inherits",
      operation: "Insert",
      via_column: "projectId",
    });
    expect(compiled.todos.delete?.using).toEqual({
      type: "Inherits",
      operation: "Delete",
      via_column: "projectId",
    });
  });

  it("supports bounded recursive inherits depth override", () => {
    const compiled = definePermissions(app, ({ policy, allowedTo }) => [
      policy.todos.allowRead.where(allowedTo.read("projectId", { maxDepth: 3 })),
    ]);

    expect(compiled.todos.select?.using).toEqual({
      type: "Inherits",
      operation: "Select",
      via_column: "projectId",
      max_depth: 3,
    });
  });

  it("rejects invalid recursive depth overrides", () => {
    expect(() =>
      definePermissions(app, ({ policy, allowedTo }) => [
        policy.todos.allowRead.where(allowedTo.read("projectId", { maxDepth: 0 })),
      ]),
    ).toThrow(/maxdepth must be a positive integer/i);
  });

  it("compiles policy.recursive start/step with policy.exists(relation)", () => {
    const compiled = definePermissions(app, ({ policy, session }) => {
      const reachableTeams = policy.recursive({
        start: policy.teams
          .where({
            kind: "individual",
            identity_key: session.userId,
          })
          .select({ team: "id" }),
        step: ({ self }) =>
          self.join(policy.team_team_edges, { left: "team", right: "child_team" }).select({
            team: "parent_team",
          }),
        maxDepth: 3,
      });

      const hasResourceRole = (resource: unknown, role: string) =>
        policy.exists(
          reachableTeams.join(policy.resource_access_edges, { left: "team", right: "team" }).where({
            "resource_access_edges.resource": resource,
            grant_role: role,
          }),
        );

      return [policy.todos.allowRead.where((todo) => hasResourceRole(todo.id, "viewer"))];
    });

    const using = compiled.todos.select?.using;
    expect(using?.type).toBe("Exists");
    if (!using || using.type !== "Exists") {
      throw new Error("Expected compiled recursive expression to be EXISTS.");
    }
    expect(using.table).toBe("resource_access_edges");
    expect(using.condition.type).toBe("And");
    if (using.condition.type !== "And") {
      throw new Error("Expected anchor EXISTS condition to be AND.");
    }

    expect(using.condition.exprs).toContainEqual({
      type: "Cmp",
      column: "resource",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["__jazz_outer_row", "id"],
      },
    });
    expect(using.condition.exprs).toContainEqual({
      type: "Cmp",
      column: "grant_role",
      op: "Eq",
      value: {
        type: "Literal",
        value: "viewer",
      },
    });

    const recursiveExpr = using.condition.exprs.find((expr) => expr.type === "Or");
    expect(recursiveExpr?.type).toBe("Or");
    if (!recursiveExpr || recursiveExpr.type !== "Or") {
      throw new Error("Expected recursive reachability OR expression.");
    }
    expect(recursiveExpr.exprs).toHaveLength(4);
  });

  it("compiles gather/hopTo recursive relation with policy.exists(relation)", () => {
    const compiled = definePermissions(app, ({ policy, session }) => {
      const reachableTeams = policy.teams.gather({
        start: {
          kind: "individual",
          identity_key: session.userId,
        },
        step: ({ current }) =>
          policy.team_team_edges.where({ child_team: current }).hopTo("parent_team"),
        maxDepth: 3,
      });

      const hasResourceRole = (resource: unknown, role: string) =>
        policy.exists(
          reachableTeams.hopTo("resource_access_edgesViaTeam").where({
            "resource_access_edges.resource": resource,
            grant_role: role,
          }),
        );

      return [policy.todos.allowRead.where((todo) => hasResourceRole(todo.id, "viewer"))];
    });

    const using = compiled.todos.select?.using;
    expect(using?.type).toBe("Exists");
    if (!using || using.type !== "Exists") {
      throw new Error("Expected compiled recursive expression to be EXISTS.");
    }
    expect(using.table).toBe("resource_access_edges");
    expect(using.condition.type).toBe("And");
    if (using.condition.type !== "And") {
      throw new Error("Expected anchor EXISTS condition to be AND.");
    }

    expect(using.condition.exprs).toContainEqual({
      type: "Cmp",
      column: "resource",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["__jazz_outer_row", "id"],
      },
    });
    expect(using.condition.exprs).toContainEqual({
      type: "Cmp",
      column: "grant_role",
      op: "Eq",
      value: {
        type: "Literal",
        value: "viewer",
      },
    });

    const recursiveExpr = using.condition.exprs.find((expr) => expr.type === "Or");
    expect(recursiveExpr?.type).toBe("Or");
    if (!recursiveExpr || recursiveExpr.type !== "Or") {
      throw new Error("Expected recursive reachability OR expression.");
    }
    expect(recursiveExpr.exprs).toHaveLength(4);
  });

  it("rejects invalid gather(...) step shapes", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => {
        const reachableTeams = policy.teams.gather({
          start: { kind: "individual" },
          step: ({ current }) => policy.team_team_edges.where({ child_team: current }),
        });
        return [policy.todos.allowRead.where(policy.exists(reachableTeams))];
      }),
    ).toThrow(/step must include exactly one hopto/i);

    expect(() =>
      definePermissions(app, ({ policy }) => {
        const reachableTeams = policy.teams.gather({
          start: { kind: "individual" },
          step: () => policy.team_team_edges.where({ child_team: "literal" }).hopTo("parent_team"),
        });
        return [policy.todos.allowRead.where(policy.exists(reachableTeams))];
      }),
    ).toThrow(/where condition bound to current/i);
  });

  it("rejects invalid policy.recursive start/step shapes", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => {
        const reachableTeams = policy.recursive({
          start: policy.teams.where({ kind: "individual" }),
          step: ({ self }) =>
            self.join(policy.team_team_edges, { left: "team", right: "child_team" }).select({
              team: "parent_team",
            }),
        });
        return [policy.todos.allowRead.where(policy.exists(reachableTeams))];
      }),
    ).toThrow(/start must project exactly one column/i);

    expect(() =>
      definePermissions(app, ({ policy, session }) => {
        const reachableTeams = policy.recursive({
          start: policy.teams.where({ identity_key: session.userId }).select({ team: "id" }),
          step: ({ self }) =>
            self.join(policy.team_team_edges, { left: "team", right: "child_team" }).select({
              wrong_alias: "parent_team",
            }),
        });
        return [policy.todos.allowRead.where(policy.exists(reachableTeams))];
      }),
    ).toThrow(/step select alias must match start alias/i);

    expect(() =>
      definePermissions(app, ({ policy, session }) => {
        const reachableTeams = policy.recursive({
          start: policy.teams.where({ identity_key: session.userId }).select({ team: "id" }),
          step: ({ self }) =>
            self.join(policy.team_team_edges, { left: "team", right: "child_team" }).select({
              team: "parent_team",
            }),
          maxDepth: 999,
        });
        return [policy.todos.allowRead.where(policy.exists(reachableTeams))];
      }),
    ).toThrow(/exceeds hard cap/i);
  });

  it("rejects allowedTo when column is not a foreign key", () => {
    expect(() =>
      definePermissions(app, ({ policy, allowedTo }) => [
        policy.todos.allowRead.where(allowedTo.read("ownerId")),
      ]),
    ).toThrow(/available fk columns: projectId/i);
  });

  it("rejects allowedTo when app.wasmSchema metadata is missing", () => {
    expect(() =>
      definePermissions(appWithoutSchema, ({ policy, allowedTo }) => [
        policy.todos.allowRead.where(allowedTo.read("projectId")),
      ]),
    ).toThrow(/table metadata is missing in app\.wasmSchema/i);
  });

  it("rejects row references outside exists clauses", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where((todo) => ({ ownerId: todo.id })),
      ]),
    ).toThrow(/row references are only valid inside exists\(\) clauses/i);
  });

  it("supports update rules with only whereOld or whereNew", () => {
    const oldOnly = definePermissions(app, ({ policy, session }) => [
      policy.todos.allowUpdate.whereOld({ ownerId: session.userId }),
    ]);
    expect(oldOnly.todos.update?.using).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
    expect(oldOnly.todos.update?.with_check).toEqual(oldOnly.todos.update?.using);

    const newOnly = definePermissions(app, ({ policy, session }) => [
      policy.todos.allowUpdate.whereNew({ ownerId: session.userId }),
    ]);
    expect(newOnly.todos.update?.with_check).toEqual({
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
    });
    expect(newOnly.todos.update?.using).toEqual(newOnly.todos.update?.with_check);
  });

  it("rejects unsupported where operators and invalid compound combinator inputs", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where({ done: { contains: true } } as unknown as TodoWhere),
      ]),
    ).toThrow(/where operator "contains" is not yet supported/i);

    expect(() =>
      definePermissions(app, ({ policy, allOf }) => [
        policy.todos.allowRead.where(allOf({ done: true } as unknown as readonly unknown[])),
      ]),
    ).toThrow(/allOf\(\.\.\.\).*array/i);

    expect(() =>
      definePermissions(app, ({ policy, anyOf }) => [
        policy.todos.allowRead.where(anyOf({ done: true } as unknown as readonly unknown[])),
      ]),
    ).toThrow(/anyOf\(\.\.\.\).*array/i);
  });

  it("compiles correlated exists row references", () => {
    const compiled = definePermissions(app, ({ policy, anyOf, allowedTo, session }) => [
      policy.todos.allowRead.where((todo) =>
        anyOf([
          allowedTo.read("projectId"),
          policy.todoShares.exists.where({
            todoId: todo.id,
            userId: session.userId,
            canRead: true,
          }),
        ]),
      ),
    ]);

    expect(compiled.todos.select?.using).toEqual({
      type: "Or",
      exprs: [
        {
          type: "Inherits",
          operation: "Select",
          via_column: "projectId",
        },
        {
          type: "Exists",
          table: "todoShares",
          condition: {
            type: "And",
            exprs: [
              {
                type: "Cmp",
                column: "todoId",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["__jazz_outer_row", "id"],
                },
              },
              {
                type: "Cmp",
                column: "userId",
                op: "Eq",
                value: {
                  type: "SessionRef",
                  path: ["userId"],
                },
              },
              {
                type: "Cmp",
                column: "canRead",
                op: "Eq",
                value: {
                  type: "Literal",
                  value: true,
                },
              },
            ],
          },
        },
      ],
    });
  });

  it("validates inherits against the exists table when nested in raw PolicyExpr", () => {
    const manualExistsExpr: PolicyExpr = {
      type: "Exists",
      table: "todoShares",
      condition: {
        type: "Inherits",
        operation: "Select",
        via_column: "todoId",
      },
    };

    const compiled = definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.where(manualExistsExpr),
    ]);

    expect(compiled.todos.select?.using).toEqual(manualExistsExpr);
  });

  it("rejects invalid nested inherits columns against exists table metadata", () => {
    const invalidManualExistsExpr: PolicyExpr = {
      type: "Exists",
      table: "todoShares",
      condition: {
        type: "Inherits",
        operation: "Select",
        via_column: "projectId",
      },
    };

    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where(invalidManualExistsExpr),
      ]),
    ).toThrow(/available fk columns: todoId/i);
  });
});
