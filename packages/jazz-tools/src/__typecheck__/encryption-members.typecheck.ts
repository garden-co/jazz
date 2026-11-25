import type { Db } from "../runtime/db.js";
import { app } from "../../tests/fixtures/encrypted/jazz.generated.js";

declare const db: Db;
declare const id: string;
declare const accountId: string;

db.encryption.members.add(app.projects, id, accountId).wait({ tier: "global" });
db.encryption.members.list(app.projects, id);
db.encryption.members.remove(app.projects, id, accountId);

// @ts-expect-error tasks belong to a project and do not define their own spaces.
db.encryption.members.add(app.tasks, id, accountId);
// @ts-expect-error tasks are not a space table.
db.encryption.members.list(app.tasks, id);
// @ts-expect-error tasks are not a space table.
db.encryption.members.remove(app.tasks, id, accountId);

db.transaction((tx) => {
  tx.encryption.members.add(app.projects, id, accountId).accountId;
  // @ts-expect-error tasks are not a space table.
  tx.encryption.members.add(app.tasks, id, accountId);
});
