import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy }) => [
  policy.messages.allowRead.always(),
  policy.better_auth_user.allowRead.never(),
  policy.better_auth_session.allowRead.never(),
]);
