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

const app = {
  todos: new TodoQueryBuilder(),
  projects: new ProjectQueryBuilder(),
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
    },
  },
} as const;

describe("permissions type inference", () => {
  it("infers row callback and where key types", () => {
    definePermissions(app, ({ policy, anyOf, allowedTo, session }) => {
      expectTypeOf(session.userId.path).toEqualTypeOf<string[]>();

      return [
        policy.todos.allowRead.where((todo) =>
          anyOf([
            { done: false },
            policy.projects.exists.where({
              id: todo.projectId,
              ownerId: session.userId,
            }),
          ]),
        ),
        policy.todos.allowUpdate
          .whereOld(allowedTo.update("projectId"))
          .whereNew(allowedTo.update("projectId")),
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
