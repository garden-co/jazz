import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy }) => {
  policy.documents.allowRead.always();
  policy.documents.allowInsert.always();
  policy.documents.allowUpdate.always();
});
