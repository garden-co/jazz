import { type Db } from "../../src/runtime/db.js";
import { app, Project, Todo, User } from "./fixtures/basic/app";

export function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function insertUser(db: Db, name = "Test User"): User {
  return db.insert(app.users, { name, friends: [] });
}

export function insertProject(db: Db, name = "Test Project"): Project {
  return db.insert(app.projects, { name });
}

export function insertTodo(db: Db, data: Partial<Todo>): Todo {
  return db.insert(app.todos, {
    title: data.title ?? "Test Todo",
    done: data.done ?? false,
    tags: data.tags ?? [],
    project: data.project ?? insertProject(db).id,
    owner: data.owner ?? undefined,
    assignees: data.assignees ?? [],
  });
}
