import { schema as s, type RowRefValue } from "jazz-tools";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema";

const wequencerPermissions = s.definePermissions(
  app,
  ({ policy, session, anyOf, allOf, allowedTo }) => {
    const isMember = (sessionId: RowRefValue) =>
      policy.session_members.exists.where({ session_id: sessionId, member_author: session.user });
    const canEdit = (sessionId: RowRefValue) =>
      anyOf([
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.user,
          role: "editor",
        }),
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.user,
          role: "owner",
        }),
      ]);
    // Administrative authority is the immutable system author of the session
    // row. `owner` is only the creator's initial collaboration role: deleting
    // or replacing that mutable membership row must not transfer or revoke the
    // creator's ability to administer the session.
    const isCreator = (sessionId: RowRefValue) =>
      policy.sessions.exists.where({ id: sessionId, $createdBy: session.user });
    policy.profiles.allowRead.where({ author: session.user });
    policy.profiles.allowInsert.where({ author: session.user });
    policy.profiles.allowUpdate
      .whereOld({ author: session.user })
      .whereNew({ author: session.user });
    policy.profiles.allowDelete.where({ author: session.user });
    policy.sessions.allowRead.where((row) =>
      anyOf([{ $createdBy: session.user }, isMember(row.id)]),
    );
    policy.sessions.allowInsert.always();
    policy.sessions.allowUpdate.where({ $createdBy: session.user });
    policy.sessions.allowDelete.where({ $createdBy: session.user });
    policy.session_members.allowRead.where(allowedTo.read("session_id"));
    policy.session_members.allowInsert.where(allowedTo.update("session_id"));
    policy.session_members.allowUpdate.never();
    policy.session_members.allowDelete.where(allowedTo.update("session_id"));
    policy.tracks.allowRead.where((row) => isMember(row.session_id));
    policy.tracks.allowInsert.where((row) => canEdit(row.session_id));
    policy.tracks.allowUpdate.where((row) => canEdit(row.session_id));
    policy.tracks.allowDelete.where((row) => isCreator(row.session_id));
    policy.steps.allowRead.where(allowedTo.read("track_id"));
    policy.steps.allowInsert.where(allowedTo.update("track_id"));
    policy.steps.allowUpdate.where(allowedTo.update("track_id"));
    policy.steps.allowDelete.where(allowedTo.update("track_id"));
    policy.transport_observations.allowRead.where((row) => isMember(row.session_id));
    policy.transport_observations.allowInsert.where((row) => canEdit(row.session_id));
    policy.transport_observations.allowUpdate.where((row) => canEdit(row.session_id));
    policy.transport_observations.allowDelete.where((row) => canEdit(row.session_id));
    policy.presence.allowRead.where((row) => isMember(row.session_id));
    policy.presence.allowInsert.where((row) =>
      allOf([isMember(row.session_id), allowedTo.update("profile_id")]),
    );
    policy.presence.allowUpdate.where((row) =>
      allOf([isMember(row.session_id), allowedTo.update("profile_id")]),
    );
    policy.presence.allowDelete.where({ $createdBy: session.user });
  },
);

export default { ...betterAuthPermissions, ...wequencerPermissions };
