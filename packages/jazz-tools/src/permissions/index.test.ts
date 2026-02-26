import { describe, expect, it } from "vitest";
import {
  definePermissions,
  relationExistsToPolicy,
  relationToIr,
  type PermissionRelation,
} from "./index.js";
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

interface Profile {
  id: string;
}

interface ProfileWhere {
  id?: string;
}

interface Person {
  id: string;
  profileId?: string;
}

interface PersonWhere {
  id?: string;
  profileId?: string;
}

interface Friendship {
  id: string;
  personAId: string;
  personBId: string;
}

interface FriendshipWhere {
  id?: string;
  personAId?: string;
  personBId?: string;
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

class ProfileQueryBuilder {
  declare readonly _rowType: Profile;
  where(_input: ProfileWhere): ProfileQueryBuilder {
    return this;
  }
}

class PersonQueryBuilder {
  declare readonly _rowType: Person;
  where(_input: PersonWhere): PersonQueryBuilder {
    return this;
  }
}

class FriendshipQueryBuilder {
  declare readonly _rowType: Friendship;
  where(_input: FriendshipWhere): FriendshipQueryBuilder {
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

const socialApp = {
  profiles: new ProfileQueryBuilder(),
  people: new PersonQueryBuilder(),
  friendships: new FriendshipQueryBuilder(),
  wasmSchema: {
    tables: {
      profiles: {
        columns: [{ name: "id", column_type: { type: "Uuid" }, nullable: false }],
      },
      people: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          {
            name: "profileId",
            column_type: { type: "Uuid" },
            nullable: true,
            references: "profiles",
          },
        ],
      },
      friendships: {
        columns: [
          { name: "id", column_type: { type: "Uuid" }, nullable: false },
          {
            name: "personAId",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "people",
          },
          {
            name: "personBId",
            column_type: { type: "Uuid" },
            nullable: false,
            references: "people",
          },
        ],
      },
    },
  },
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

  it("supports allowedTo.readReferencing helper", () => {
    const compiled = definePermissions(app, ({ policy, allowedTo }) => [
      policy.projects.allowRead.where(allowedTo.readReferencing(policy.todos, "projectId")),
    ]);

    expect(compiled.projects.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "todos",
      via_column: "projectId",
    });
  });

  it("supports bounded recursive referencing inherits depth override", () => {
    const compiled = definePermissions(app, ({ policy, allowedTo }) => [
      policy.projects.allowRead.where(
        allowedTo.readReferencing(policy.todos, "projectId", { maxDepth: 3 }),
      ),
    ]);

    expect(compiled.projects.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "todos",
      via_column: "projectId",
      max_depth: 3,
    });
  });

  it("rejects referencing inherits when source FK does not target current table", () => {
    expect(() =>
      definePermissions(app, ({ policy, allowedTo }) => [
        policy.projects.allowRead.where(allowedTo.readReferencing(policy.todoShares, "todoId")),
      ]),
    ).toThrow(/references "todos" but this rule is for "projects"/i);
  });

  it("supports split friend-profile chain style (friendships + readReferencing)", () => {
    const compiled = definePermissions(socialApp, ({ policy, anyOf, allowedTo, session }) => [
      policy.people.allowRead.where((person) =>
        anyOf([
          policy.friendships.exists.where({
            personAId: session.personId,
            personBId: person.id,
          }),
          policy.friendships.exists.where({
            personBId: session.personId,
            personAId: person.id,
          }),
        ]),
      ),
      policy.profiles.allowRead.where(allowedTo.readReferencing(policy.people, "profileId")),
    ]);

    expect(compiled.profiles.select?.using).toEqual({
      type: "InheritsReferencing",
      operation: "Select",
      source_table: "people",
      via_column: "profileId",
    });

    const peopleUsing = compiled.people.select?.using;
    expect(peopleUsing?.type).toBe("Or");
    if (!peopleUsing || peopleUsing.type !== "Or") {
      throw new Error("Expected people policy to compile to OR.");
    }
    expect(peopleUsing.exprs).toHaveLength(2);
    for (const expr of peopleUsing.exprs) {
      expect(expr.type).toBe("Exists");
      if (expr.type !== "Exists") {
        throw new Error("Expected OR branch to be Exists.");
      }
      expect(expr.table).toBe("friendships");
    }
  });

  it("supports one-clause friend-profile chain style using join(...)", () => {
    const compiled = definePermissions(socialApp, ({ policy, anyOf, session }) => [
      policy.profiles.allowRead.where((profile) =>
        anyOf([
          policy.exists(
            policy.people
              .where({ profileId: profile.id })
              .join(policy.friendships, { left: "id", right: "personAId" })
              .where({ personBId: session.personId }),
          ),
          policy.exists(
            policy.people
              .where({ profileId: profile.id })
              .join(policy.friendships, { left: "id", right: "personBId" })
              .where({ personAId: session.personId }),
          ),
        ]),
      ),
    ]);

    const using = compiled.profiles.select?.using;
    expect(using?.type).toBe("Or");
    if (!using || using.type !== "Or") {
      throw new Error("Expected profile policy to compile to OR.");
    }
    expect(using.exprs).toHaveLength(2);
    for (const branch of using.exprs) {
      expect(branch.type).toBe("ExistsRel");
      if (branch.type !== "ExistsRel") {
        throw new Error("Expected OR branch to be ExistsRel.");
      }
      expect(branch.rel.type).toBe("Filter");
      if (branch.rel.type !== "Filter") {
        throw new Error("Expected ExistsRel relation to be Filter.");
      }
      expect(branch.rel.input.type).toBe("Join");
      expect(JSON.stringify(branch.rel)).toContain('"type":"OuterColumn"');
    }
  });

  it("supports one-clause friend-profile chain style using hopTo(...).where(...)", () => {
    const compiled = definePermissions(socialApp, ({ policy, anyOf, session }) => [
      policy.profiles.allowRead.where((profile) =>
        anyOf([
          policy.exists(
            policy.people
              .where({ profileId: profile.id })
              .hopTo("friendshipsViaPersonAId")
              .where({ personBId: session.personId }),
          ),
          policy.exists(
            policy.people
              .where({ profileId: profile.id })
              .hopTo("friendshipsViaPersonBId")
              .where({ personAId: session.personId }),
          ),
        ]),
      ),
    ]);

    const using = compiled.profiles.select?.using;
    expect(using?.type).toBe("Or");
    if (!using || using.type !== "Or") {
      throw new Error("Expected profile policy to compile to OR.");
    }
    expect(using.exprs).toHaveLength(2);
    for (const branch of using.exprs) {
      expect(branch.type).toBe("ExistsRel");
      if (branch.type !== "ExistsRel") {
        throw new Error("Expected OR branch to be ExistsRel.");
      }
      expect(branch.rel.type).toBe("Project");
      if (branch.rel.type !== "Project") {
        throw new Error("Expected ExistsRel relation to be Project.");
      }
      expect(branch.rel.input.type).toBe("Filter");
      if (branch.rel.input.type !== "Filter") {
        throw new Error("Expected hop relation filter.");
      }
      expect(branch.rel.input.input.type).toBe("Join");
      expect(JSON.stringify(branch.rel)).toContain('"type":"OuterColumn"');
    }
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
    expect(using?.type).toBe("ExistsRel");
    if (!using || using.type !== "ExistsRel") {
      throw new Error("Expected compiled recursive expression to be ExistsRel.");
    }
    expect(using.rel.type).toBe("Project");
    if (using.rel.type !== "Project") {
      throw new Error("Expected projected relation IR.");
    }
    expect(using.rel.input.type).toBe("Filter");
    if (using.rel.input.type !== "Filter") {
      throw new Error("Expected filtered relation IR.");
    }
    expect(using.rel.input.input.type).toBe("Join");
    if (using.rel.input.input.type !== "Join") {
      throw new Error("Expected relation IR join.");
    }
    expect(using.rel.input.input.left.type).toBe("Gather");
  });

  it("lowers hop relation plans to relation IR join + project", () => {
    let relation: PermissionRelation | undefined;
    definePermissions(app, ({ policy }) => {
      relation = policy.team_team_edges.where({ child_team: "team-a" }).hopTo("parent_team");
      return [];
    });
    if (!relation) {
      throw new Error("Expected relation to be initialized.");
    }

    const ir = relationToIr(relation);
    expect(ir.type).toBe("Project");
    if (ir.type !== "Project") {
      throw new Error("Expected relation IR project.");
    }
    expect(ir.input.type).toBe("Filter");
    if (ir.input.type !== "Filter") {
      throw new Error("Expected relation IR filter.");
    }
    expect(ir.input.input.type).toBe("Join");
    if (ir.input.input.type !== "Join") {
      throw new Error("Expected relation IR join.");
    }
    expect(ir.input.input.on).toEqual([
      {
        left: { scope: "team_team_edges", column: "parent_team" },
        right: { scope: "__hop_0", column: "id" },
      },
    ]);
    expect(ir.columns).toEqual([
      {
        alias: "id",
        expr: {
          type: "Column",
          column: { scope: "__hop_0", column: "id" },
        },
      },
    ]);
  });

  it("lowers recursive relation plans to gather IR and wraps in ExistsRel policy expr", () => {
    let relation: PermissionRelation | undefined;
    definePermissions(app, ({ policy, session }) => {
      const reachableTeams = policy.teams.gather({
        start: {
          kind: "individual",
          identity_key: session.userId,
        },
        step: ({ current }) =>
          policy.team_team_edges.where({ child_team: current }).hopTo("parent_team"),
        maxDepth: 3,
      });
      relation = reachableTeams.hopTo("resource_access_edgesViaTeam").where({
        "resource_access_edges.resource": "resource-a",
        grant_role: "viewer",
      });
      return [];
    });
    if (!relation) {
      throw new Error("Expected recursive relation to be initialized.");
    }

    const ir = relationToIr(relation);
    expect(ir.type).toBe("Project");
    if (ir.type !== "Project") {
      throw new Error("Expected projected recursive relation IR.");
    }
    expect(ir.input.type).toBe("Filter");
    if (ir.input.type !== "Filter") {
      throw new Error("Expected filtered recursive relation IR.");
    }
    expect(ir.input.input.type).toBe("Join");
    if (ir.input.input.type !== "Join") {
      throw new Error("Expected recursive post-join relation IR.");
    }
    expect(ir.input.input.left.type).toBe("Gather");

    const existsExpr = relationExistsToPolicy(relation);
    expect(existsExpr).toMatchObject({
      type: "ExistsRel",
      rel: {
        type: "Project",
      },
    });
  });

  it("compiles policy.exists(relation) to ExistsRel in definePermissions", () => {
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

      return [
        policy.todos.allowRead.where(
          policy.exists(
            reachableTeams.hopTo("resource_access_edgesViaTeam").where({
              "resource_access_edges.resource": "resource-a",
              grant_role: "viewer",
            }),
          ),
        ),
      ];
    });

    expect(compiled.todos.select?.using).toMatchObject({
      type: "ExistsRel",
      rel: {
        type: "Project",
      },
    });
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

  it("supports contains and in where operators", () => {
    const containsCompiled = definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.where({ ownerId: { contains: "ali" } } as unknown as TodoWhere),
    ]);
    expect(containsCompiled.todos.select?.using).toEqual({
      type: "Contains",
      column: "ownerId",
      value: {
        type: "Literal",
        value: "ali",
      },
    });

    const inListCompiled = definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.where({ ownerId: { in: ["alice", "bob"] } } as unknown as TodoWhere),
    ]);
    expect(inListCompiled.todos.select?.using).toEqual({
      type: "InList",
      column: "ownerId",
      values: [
        {
          type: "Literal",
          value: "alice",
        },
        {
          type: "Literal",
          value: "bob",
        },
      ],
    });

    const inSessionCompiled = definePermissions(app, ({ policy, session }) => [
      policy.todos.allowRead.where({
        ownerId: { in: session["claims.teamIds"] },
      } as unknown as TodoWhere),
    ]);
    expect(inSessionCompiled.todos.select?.using).toEqual({
      type: "In",
      column: "ownerId",
      session_path: ["claims", "teamIds"],
    });

    const emptyInCompiled = definePermissions(app, ({ policy }) => [
      policy.todos.allowRead.where({ ownerId: { in: [] } } as unknown as TodoWhere),
    ]);
    expect(emptyInCompiled.todos.select?.using).toEqual({ type: "False" });
  });

  it("rejects unsupported where operators and invalid compound combinator inputs", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where({ ownerId: { in: "alice" } } as unknown as TodoWhere),
      ]),
    ).toThrow(/ownerId\.in.*array or session reference/i);

    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where({ done: { startsWith: true } } as unknown as TodoWhere),
      ]),
    ).toThrow(/unsupported where operator "startsWith"/i);

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
