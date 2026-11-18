import { schema as s } from "jazz-tools";
import { app } from "./schema";

// Unset when validate runs: alpha.64 compiled `{ room: undefined }` to `true`.
const ROOM = process.env.UPGRADE_FIXTURE_ROOM!;

export default s.definePermissions(app, ({ policy, anyOf, session }) => [
  policy.messages.allowRead.where(anyOf([{ room: ROOM }, { $createdBy: session.user }])),
  policy.messages.allowInsert.where({ room: ROOM }),
]);
