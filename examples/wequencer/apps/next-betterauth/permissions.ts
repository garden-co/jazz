import { schema as s, type RowRefValue } from "jazz-tools";
import { permissions as betterAuthPermissions } from "./schema-better-auth/schema";
import { app } from "./schema";

const wequencerPermissions = s.definePermissions(
  app,
  ({ policy, session, anyOf, allOf, allowedTo }) => {
    const isMember = (sessionId: RowRefValue) =>
      policy.session_members.exists.where({ session_id: sessionId, member_author: session.author });
    const canEdit = (sessionId: RowRefValue) =>
      anyOf([
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.author,
          role: "editor",
        }),
        policy.session_members.exists.where({
          session_id: sessionId,
          member_author: session.author,
          role: "owner",
        }),
      ]);
    const isOwner = (sessionId: RowRefValue) =>
      policy.session_members.exists.where({
        session_id: sessionId,
        member_author: session.author,
        role: "owner",
      });
    policy.profiles.allowRead.where({ author: session.author });
    policy.profiles.allowInsert.where({ author: session.author });
    policy.profiles.allowUpdate
      .whereOld({ author: session.author })
      .whereNew({ author: session.author });
    policy.profiles.allowDelete.where({ author: session.author });
    policy.sessions.allowRead.where((row) =>
      anyOf([{ $createdBy: session.author }, isMember(row.id)]),
    );
    policy.sessions.allowInsert.always();
    policy.sessions.allowUpdate.where({ $createdBy: session.author });
    policy.sessions.allowDelete.where({ $createdBy: session.author });
    policy.session_members.allowRead.where(allowedTo.read("session_id"));
    policy.session_members.allowInsert.where(allowedTo.update("session_id"));
    policy.session_members.allowUpdate.never();
    policy.session_members.allowDelete.where(allowedTo.update("session_id"));
    policy.tracks.allowRead.where((row) => isMember(row.session_id));
    policy.tracks.allowInsert.where((row) => canEdit(row.session_id));
    policy.tracks.allowUpdate.where((row) => canEdit(row.session_id));
    policy.tracks.allowDelete.where((row) => isOwner(row.session_id));
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
    policy.presence.allowDelete.where({ $createdBy: session.author });
  },
);

export default { ...betterAuthPermissions, ...wequencerPermissions };
