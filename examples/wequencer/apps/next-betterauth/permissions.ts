import { schema as s, type RowRefValue } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session, anyOf, allOf, allowedTo }) => {
  const isMember = (sessionId: RowRefValue) =>
    policy.session_members.exists.where({ session_id: sessionId, user_id: session.user_id });
  const canEdit = (sessionId: RowRefValue) =>
    anyOf([
      policy.session_members.exists.where({
        session_id: sessionId,
        user_id: session.user_id,
        role: "editor",
      }),
      policy.session_members.exists.where({
        session_id: sessionId,
        user_id: session.user_id,
        role: "owner",
      }),
    ]);
  const isOwner = (sessionId: RowRefValue) =>
    policy.session_members.exists.where({
      session_id: sessionId,
      user_id: session.user_id,
      role: "owner",
    });

  policy.profiles.allowRead.where({ user_id: session.user_id });
  policy.profiles.allowInsert.where({ user_id: session.user_id });
  policy.profiles.allowUpdate.where({ user_id: session.user_id });

  policy.sessions.allowRead.where((row) => isMember(row.id));
  policy.sessions.allowInsert.always();
  policy.sessions.allowUpdate.where((row) => isOwner(row.id));
  policy.sessions.allowDelete.where((row) => isOwner(row.id));

  policy.session_members.allowRead.where((row) => isMember(row.session_id));
  policy.session_members.allowInsert.where((row) =>
    policy.sessions.exists.where({ id: row.session_id, $createdBy: session.user_id }),
  );
  policy.session_members.allowUpdate.where((row) => isOwner(row.session_id));
  policy.session_members.allowDelete.where((row) => isOwner(row.session_id));

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

  policy.presence.allowRead.where((row) => isMember(row.session_id));
  policy.presence.allowInsert.where((row) =>
    allOf([isMember(row.session_id), allowedTo.update("profile_id")]),
  );
  policy.presence.allowUpdate.where((row) =>
    allOf([isMember(row.session_id), allowedTo.update("profile_id")]),
  );
  policy.presence.allowDelete.where({ $createdBy: session.user_id });
});
