import { schema as s } from "jazz-tools";
import { app } from "./schema";
export default s.definePermissions(app, ({ policy, session }) => {
  policy.tours.allowRead.where({ band_id: session.user_id });
  policy.tours.allowInsert.where({ band_id: session.user_id });
  policy.tours.allowUpdate.where({ band_id: session.user_id });
  for (const table of [
    policy.venues,
    policy.members,
    policy.legs,
    policy.events,
    policy.travel_days,
  ]) {
    table.allowRead.always();
    table.allowInsert.always();
    table.allowUpdate.always();
  }
});
