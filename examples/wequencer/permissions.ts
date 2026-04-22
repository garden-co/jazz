import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, isCreator }) => {
  for (const table of [
    policy.instruments,
    policy.jams,
    policy.beats,
    policy.files,
    policy.file_parts,
  ]) {
    table.allowRead.always();
    table.allowInsert.always();
    table.allowUpdate.always();
    table.allowDelete.always();
  }

  // Participants are owned by the user who created them.
  policy.participants.allowRead.always();
  policy.participants.allowInsert.always();
  policy.participants.allowUpdate.where(isCreator);
  policy.participants.allowDelete.where(isCreator);
});
