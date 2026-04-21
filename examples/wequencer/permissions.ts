import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session, allOf, isCreator }) => {
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
  policy.participants.allowInsert.where({ userId: session.user_id });
  policy.participants.allowUpdate.where(allOf([{ userId: session.user_id }, isCreator]));
  policy.participants.allowDelete.where(allOf([{ userId: session.user_id }, isCreator]));
});
