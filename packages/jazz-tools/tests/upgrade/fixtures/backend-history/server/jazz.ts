import { acceptDeviceAccounts, createJazz, memory } from "jazz-tools/backend";
import { app as schemaApp, type Note } from "../schema.js";
import permissions from "../permissions.js";

export const jazz = createJazz({
  app: schemaApp,
  permissions,
  authenticate: acceptDeviceAccounts,
  store: memory(),
});

export function title(note: Note): string {
  return note.title;
}
