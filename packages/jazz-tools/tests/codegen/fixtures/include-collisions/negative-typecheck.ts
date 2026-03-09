import { app, type TodoInclude } from "./app.ts";

declare function read<T>(_value: T): void;

const forwarded: TodoInclude = {};
const maybeForwarded = app.todos.include(forwarded).orderBy("id", "asc");
declare const row: typeof maybeForwarded._rowType;

read(row.owner?.id);
