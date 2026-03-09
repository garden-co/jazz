import { app, type TodoInclude } from "./app.ts";

const forwarded: TodoInclude = {};
const maybeForwarded = app.todos.include(forwarded).orderBy("id", "asc");
declare const row: typeof maybeForwarded._rowType;

row.owner?.id;
