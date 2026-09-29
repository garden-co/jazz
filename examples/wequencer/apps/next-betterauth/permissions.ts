import { schema as s, type RowRefValue } from "jazz-tools";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema";

const wequencerPermissions = s.definePermissions(
  app,
  ({ policy, session, anyOf, allOf, allowedTo }) => {
    const isMember = (sessionId: RowRefValue) =>
      policy.session_members.exists.where({
        session_id: sessionId,
        member_author: session.user.account,
      });
    const canEdit = (sessionId: RowRefValue) =>
      anyOf([
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.user.account,
          role: "editor",
        }),
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.user.account,
          role: "owner",
        }),
      ]);
    // Administrative authority is the immutable system author of the session
    // row. `owner` is only the creator's initial collaboration role: deleting
    // or replacing that mutable membership row must not transfer or revoke the
    // creator's ability to administer the session.
    const isCreator = (sessionId: RowRefValue) =>
      policy.sessions.exists.where({ id: sessionId, "$createdBy.account": session.user.account });
    // Bandmates see each other's display name once that profile has shown
    // presence in a session they can both read. Presence stays advisory: it
    // only reveals a name, never grants a write.
    policy.profiles.allowRead.where(
      anyOf([{ author: session.user.account }, allowedTo.read("presenceViaProfile")]),
    );
    policy.profiles.allowInsert.where({ author: session.user.account });
    policy.profiles.allowUpdate
      .whereOld({ author: session.user.account })
      .whereNew({ author: session.user.account });
    policy.profiles.allowDelete.where({ author: session.user.account });
    policy.sessions.allowRead.where((row) =>
      anyOf([{ "$createdBy.account": session.user.account }, isMember(row.id)]),
    );
    policy.sessions.allowInsert.always();
    policy.sessions.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.sessions.allowDelete.where({ "$createdBy.account": session.user.account });
    policy.session_members.allowRead.where(allowedTo.read("session"));
    policy.session_members.allowInsert.where(allowedTo.update("session"));
    policy.session_members.allowUpdate.never();
    policy.session_members.allowDelete.where(allowedTo.update("session"));
    policy.tracks.allowRead.where((row) => isMember(row.session_id));
    policy.tracks.allowInsert.where((row) => canEdit(row.session_id));
    policy.tracks.allowUpdate.where((row) => canEdit(row.session_id));
    policy.tracks.allowDelete.where((row) => isCreator(row.session_id));
    policy.patterns.allowRead.where((row) => isMember(row.session_id));
    policy.patterns.allowInsert.where((row) => canEdit(row.session_id));
    policy.patterns.allowUpdate.where((row) => canEdit(row.session_id));
    policy.patterns.allowDelete.where((row) => isCreator(row.session_id));
    policy.steps.allowRead.where(allowedTo.read("track"));
    // A step joins one track and one pattern; writing it needs edit access to
    // both, so a step cannot be attached to another session's pattern.
    policy.steps.allowInsert.where(allOf([allowedTo.update("track"), allowedTo.update("pattern")]));
    policy.steps.allowUpdate.where(allOf([allowedTo.update("track"), allowedTo.update("pattern")]));
    policy.steps.allowDelete.where(allowedTo.update("track"));
    policy.transport_observations.allowRead.where((row) => isMember(row.session_id));
    policy.transport_observations.allowInsert.where((row) => canEdit(row.session_id));
    policy.transport_observations.allowUpdate.where((row) => canEdit(row.session_id));
    policy.transport_observations.allowDelete.where((row) => canEdit(row.session_id));
    policy.presence.allowRead.where((row) => isMember(row.session_id));
    policy.presence.allowInsert.where((row) =>
      allOf([isMember(row.session_id), allowedTo.update("profile")]),
    );
    policy.presence.allowUpdate.where((row) =>
      allOf([isMember(row.session_id), allowedTo.update("profile")]),
    );
    policy.presence.allowDelete.where({ "$createdBy.account": session.user.account });
  },
);

export default { ...betterAuthPermissions, ...wequencerPermissions };
