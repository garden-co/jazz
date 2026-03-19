import { type Db } from "../../src/runtime/db.js";
import { app, type Project, type Todo, type User } from "./fixtures/basic/schema";

export function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function insertUser(db: Db, name = "Test User"): User {
  return db.insert(app.users, { name, friendsIds: [] });
}

export function insertProject(db: Db, name = "Test Project"): Project {
  return db.insert(app.projects, { name });
}

export function insertTodo(db: Db, data: Partial<Todo>): Todo {
  return db.insert(app.todos, {
    title: data.title ?? "Test Todo",
    done: data.done ?? false,
    tags: data.tags ?? [],
    projectId: data.projectId ?? insertProject(db).id,
    ownerId: data.ownerId ?? undefined,
    assigneesIds: data.assigneesIds ?? [],
  });
}
