import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy }) => [
  policy.tasks.allowRead.always(),
  policy.tasks.allowInsert.always(),
  policy.tasks.allowUpdate.always(),
  policy.tasks.allowDelete.always(),
]);
