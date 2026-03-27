import { definePermissions } from "jazz-tools/permissions";
import { app } from "./app.js";

export default definePermissions(app, ({ policy, session, anyOf }) => {
  const isBandMember = policy.members.exists.where({ userId: session.user_id });

  // Open inserts for bands, members, and venues allow the demo app to
  // bootstrap seed data without an external auth provider. In a production
  // app you'd gate these behind isBandMember or a real identity check.
  policy.bands.allowRead.where({});
  policy.bands.allowInsert.where({});
  policy.bands.allowUpdate.where(isBandMember);
  policy.bands.allowDelete.where(isBandMember);

  policy.members.allowRead.where({ userId: session.user_id });
  policy.members.allowInsert.where({});

  policy.venues.allowRead.where({});
  policy.venues.allowInsert.where({});
  policy.venues.allowUpdate.where(isBandMember);
  policy.venues.allowDelete.where(isBandMember);

  policy.stops.allowRead.where(anyOf([{ status: "confirmed" }, isBandMember]));
  policy.stops.allowInsert.where(isBandMember);
  policy.stops.allowUpdate.where(isBandMember);
  policy.stops.allowDelete.where(isBandMember);

  policy.files.allowRead.where({});
  policy.files.allowInsert.where(isBandMember);
  policy.files.allowUpdate.where(isBandMember);
  policy.files.allowDelete.where(isBandMember);

  policy.file_parts.allowRead.where({});
  policy.file_parts.allowInsert.where(isBandMember);
  policy.file_parts.allowUpdate.where(isBandMember);
  policy.file_parts.allowDelete.where(isBandMember);
});
