import { schema as s } from "jazz-tools";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, session }) => {
  policy.albums.allowRead.where({});
  policy.albums.allowInsert.always();
  policy.tracks.allowRead.where({});
  policy.tracks.allowInsert.always();
  policy.playlists.allowRead.where({ $createdBy: session.user_id });
  policy.playlists.allowInsert.always();
  policy.playlists.allowUpdate.where({ $createdBy: session.user_id });
  policy.playlist_entries.allowRead.where({ $createdBy: session.user_id });
  policy.playlist_entries.allowInsert.always();
  policy.invitations.allowRead.where({ subject: session.user_id });
  policy.invitations.allowInsert.always();
  policy.playback_positions.allowRead.where({ $createdBy: session.user_id });
  policy.playback_positions.allowInsert.always();
});
