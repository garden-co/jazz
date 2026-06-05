import { type Db } from "../../src/runtime/db.js";
import { app, type Project, type Todo, type User } from "./fixtures/basic/schema";

export function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function insertUser(db: Db, name = "Test User"): User {
  const { value: user } = db.insert(app.users, { name, friendsIds: [] });
  return user;
}

export function insertProject(db: Db, name = "Test Project"): Project {
  const { value: project } = db.insert(app.projects, { name });
  return project;
}

export function insertTodo(db: Db, data: Partial<Todo>): Todo {
  const { value: todo } = db.insert(app.todos, {
    title: data.title ?? "Test Todo",
    done: data.done ?? false,
    tags: data.tags ?? [],
    checkpoints: data.checkpoints ?? [],
    flags: data.flags ?? [],
    projectId: data.projectId ?? insertProject(db).id,
    ownerId: data.ownerId,
    assigneesIds: data.assigneesIds ?? [],
  });
  return todo;
}
