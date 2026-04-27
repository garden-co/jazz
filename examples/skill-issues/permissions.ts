import { schema as s } from "jazz-tools";
import { app } from "./schema.js";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.users.allowRead.always();
  policy.users.allowInsert.never();
  policy.users.allowUpdate.never();
  policy.users.allowDelete.never();

  policy.items.allowRead.always();
  policy.itemStates.allowRead.always();

  const isVerifiedUser = policy.users.exists.where({ jazzUserId: session.user_id });

  policy.items.allowInsert.where(isVerifiedUser);
  policy.items.allowUpdate.where(isVerifiedUser);
  policy.items.allowDelete.where(isVerifiedUser);

  policy.itemStates.allowInsert.where(isVerifiedUser);
  policy.itemStates.allowUpdate.where(isVerifiedUser);
  policy.itemStates.allowDelete.where(isVerifiedUser);
});
