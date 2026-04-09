import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, allowedTo }) => {
  // Instruments and their audio files are readable by everyone, seeded once.
  policy.instruments.allowRead.always();
  policy.instruments.allowInsert.always();
  policy.instruments.allowUpdate.never();
  policy.instruments.allowDelete.always();

  // Jams are open: anyone can read, create, or update (tempo, beat count).
  policy.jams.allowRead.always();
  policy.jams.allowInsert.always();
  policy.jams.allowUpdate.always();
  policy.jams.allowDelete.never();

  // Beats are collaborative: anyone in the jam can place or remove beats.
  policy.beats.allowRead.always();
  policy.beats.allowInsert.always();
  policy.beats.allowUpdate.never();
  policy.beats.allowDelete.always();

  // Participants are public within a jam.
  policy.participants.allowRead.always();
  policy.participants.allowInsert.always();
  policy.participants.allowUpdate.always();
  policy.participants.allowDelete.never();

  // File storage: direct insert (created before parent row), inherited read/delete.
  policy.files.allowInsert.where({});
  policy.file_parts.allowInsert.where({});
  policy.files.allowUpdate.never();
  policy.file_parts.allowUpdate.never();
  policy.files.allowRead.where(allowedTo.readReferencing(policy.instruments, "soundFileId"));
  policy.file_parts.allowRead.where(allowedTo.readReferencing(policy.files, "partIds"));
  policy.files.allowDelete.where(allowedTo.deleteReferencing(policy.instruments, "soundFileId"));
  policy.file_parts.allowDelete.where(allowedTo.deleteReferencing(policy.files, "partIds"));
});
