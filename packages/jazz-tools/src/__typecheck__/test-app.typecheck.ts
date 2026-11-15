import type { Db } from "../runtime/db.js";
import type { QueryBuilder } from "../runtime/db.js";
import { testApp } from "../testing/test-app.js";

declare const db: Db;
const app = testApp({ tables: { todos: { columns: { title: "string", done: "boolean" } } } });
db.insert(app.todos, { title: "x", done: false });
app.todos
  .where({ done: false, title: { contains: "x" } })
  .include({ project: true })
  .orderBy("title");
declare function useAllLike<T extends { id: string }>(query: QueryBuilder<T>): T[];
const rows = useAllLike(app.todos.where({ done: true }));
const title: string = rows[0]!.title;
String(title);
db.update(app.todos, "id", { done: true });
