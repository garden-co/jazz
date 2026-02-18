import { describe, expect, it } from "vitest";
import { definePermissions } from "./index.js";

interface Todo {
  id: string;
  ownerId: string;
  archived: boolean;
  done: boolean;
}

interface TodoWhere {
  id?: string;
  ownerId?: string;
  archived?: boolean;
  done?: boolean;
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
  wasmSchema: {},
};

describe("permissions DSL", () => {
  it("compiles read/insert/update/delete policies", () => {
    const compiled = definePermissions(app, ({ policy, both, session }) => [
      policy.todos.allowRead.where({ ownerId: session.userId }),
      policy.todos.allowInsert.where({ ownerId: session.userId }),
      policy.todos.allowUpdate
        .whereOld(both({ ownerId: session.userId }).and({ archived: false }))
        .whereNew({ ownerId: session.userId }),
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
      type: "Cmp",
      column: "ownerId",
      op: "Eq",
      value: {
        type: "SessionRef",
        path: ["userId"],
      },
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
    const compiled = definePermissions(app, ({ policy, either, session }) => [
      policy.todos.allowReads.where({ ownerId: session.userId }),
      policy.todos.allowReads.where(either({ done: true }).or({ archived: false })),
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

  it("throws for correlated row references inside exists clauses", () => {
    expect(() =>
      definePermissions(app, ({ policy, either, session }) => [
        policy.todos.allowRead.where((todo) =>
          either({ ownerId: session.userId }).or(
            policy.todoShares.exists.where({
              todoId: todo.id,
              userId: session.userId,
              canRead: true,
            }),
          ),
        ),
      ]),
    ).toThrow(/Correlated row references in exists/);
  });
});
