import { schema } from "../../src/index.js";

/** The app the session tab tests run: one table every user may read and write. */
export const sessionTabApp = schema.defineApp(
  { tasks: schema.table({ title: schema.string() }, {}) },
  { migrations: "none" },
);

export const sessionTabPermissions = schema.definePermissions(sessionTabApp, ({ policy }) => [
  policy.tasks.allowRead.always(),
  policy.tasks.allowInsert.always(),
]);
