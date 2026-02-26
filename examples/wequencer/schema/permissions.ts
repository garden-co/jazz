import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session }) => [
  // Instruments are a shared library: everyone can read, only the seeding
  // user can insert (no update/delete needed for MVP).
  policy.instruments.allowRead.where({}),
  policy.instruments.allowInsert.where({}),

  // Jams are collaborative: any user can create, read, and update
  // (transport_start is written by whoever presses play).
  policy.jams.allowRead.where({}),
  policy.jams.allowInsert.where({}),
  policy.jams.allowUpdate.whereOld({}).whereNew({}),

  // Beats are the core sequencer grid: fully open so any participant
  // can place or remove beats in real time.
  policy.beats.allowRead.where({}),
  policy.beats.allowInsert.where({}),
  policy.beats.allowDelete.where({}),

  // Participants track who is in a jam. Anyone can join (insert) or
  // leave (delete their own entry).
  policy.participants.allowRead.where({}),
  policy.participants.allowInsert.where({}),
  policy.participants.allowDelete.where({ user_id: session.user_id }),
]);
