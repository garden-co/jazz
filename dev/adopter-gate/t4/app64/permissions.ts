import { schema as s } from "jazz-tools";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema";

const appPermissions = s.definePermissions(app, ({ policy, session }) => {
  policy.team_notes.allowRead.where({ team: session.claims["team"] });
  policy.team_notes.allowInsert.where({ team: session.claims["team"] });
  policy.team_notes.allowUpdate.never();
  policy.team_notes.allowDelete.never();

  policy.private_notes.allowRead.where({ $createdBy: session.user });
  policy.private_notes.allowInsert.where({ $createdBy: session.user });
  policy.private_notes.allowUpdate.where({ $createdBy: session.user });
  policy.private_notes.allowDelete.where({ $createdBy: session.user });
});

export default {
  ...betterAuthPermissions,
  ...appPermissions,
};
