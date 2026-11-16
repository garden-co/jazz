import { acceptDeviceAccounts, createJazz, memory } from "jazz-tools/backend";
import { app as schemaApp, type Note } from "../jazz.generated.js";
import permissions from "../jazz.permissions.json" with { type: "json" };

export const jazz = createJazz({
  app: schemaApp,
  permissions,
  authenticate: acceptDeviceAccounts,
  store: memory(),
});

export function title(note: Note): string {
  return note.title;
}
