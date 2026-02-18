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

const app = {
  todos: new TodoQueryBuilder(),
  todoShares: new TodoShareQueryBuilder(),
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
    },
  },
};

const appWithoutSchema = {
  todos: new TodoQueryBuilder(),
  todoShares: new TodoShareQueryBuilder(),
};

describe("permissions DSL", () => {
  it("compiles read/insert/update/delete policies", () => {
    const compiled = definePermissions(app, ({ policy, both, allowedTo, session }) => [
      policy.todos.allowRead.where({ ownerId: session.userId }),
      policy.todos.allowInsert.where({ ownerId: session.userId }),
      policy.todos.allowUpdate
        .whereOld(both(allowedTo.update("projectId")).and({ archived: false }))
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
    const compiled = definePermissions(app, ({ policy, either, allowedTo, session }) => [
      policy.todos.allowReads.where({ ownerId: session.userId }),
      policy.todos.allowReads.where(either({ done: true }).or(allowedTo.read("projectId"))),
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

  it("rejects unsupported where operators and invalid compound chains", () => {
    expect(() =>
      definePermissions(app, ({ policy }) => [
        policy.todos.allowRead.where({ done: { contains: true } } as unknown as TodoWhere),
      ]),
    ).toThrow(/where operator "contains" is not yet supported/i);

    expect(() =>
      definePermissions(app, ({ policy, both }) => [
        policy.todos.allowRead.where(both({ done: true }).or({ archived: false })),
      ]),
    ).toThrow(/use "either\(\.\.\.\)" for OR chains/i);

    expect(() =>
      definePermissions(app, ({ policy, either }) => [
        policy.todos.allowRead.where(either({ done: true }).and({ archived: false })),
      ]),
    ).toThrow(/use "both\(\.\.\.\)" for AND chains/i);
  });

  it("compiles correlated exists row references", () => {
    const compiled = definePermissions(app, ({ policy, either, allowedTo, session }) => [
      policy.todos.allowRead.where((todo) =>
        either(allowedTo.read("projectId")).or(
          policy.todoShares.exists.where({
            todoId: todo.id,
            userId: session.userId,
            canRead: true,
          }),
        ),
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
