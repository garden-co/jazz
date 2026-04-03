import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, allowedTo }) => {
  // Instruments and their audio files are readable by everyone, seeded once.
  policy.instruments.allowRead.always();
  policy.instruments.allowInsert.always();

  // Jams are open: anyone can read, create, or update (tempo, beat count).
  policy.jams.allowRead.always();
  policy.jams.allowInsert.always();
  policy.jams.allowUpdate.always();

  // Beats are collaborative: anyone in the jam can place or remove beats.
  policy.beats.allowRead.always();
  policy.beats.allowInsert.always();
  policy.beats.allowDelete.always();

  // Participants are public within a jam.
  policy.participants.allowRead.always();
  policy.participants.allowInsert.always();
  policy.participants.allowUpdate.always();

  // File storage: direct insert (created before parent row), inherited read/delete.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});
  policy.files.allowRead.where(allowedTo.readReferencing(policy.instruments, "soundFileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
});
