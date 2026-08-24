import { schema as s } from "jazz-tools";
import type { RowRefValue } from "jazz-tools/permissions";
import { app } from "./schema";

export default s.definePermissions(app, ({ policy, anyOf, allowedTo, session }) => {
  policy.albums.allowRead.where({});
  policy.albums.allowInsert.always();
  policy.tracks.allowRead.where({});
  policy.tracks.allowInsert.always();
  const canReadPlaylist = (playlistId: RowRefValue) =>
    anyOf([
      { $createdBy: session.author },
      policy.invitations.exists.where({
        playlist_id: playlistId,
        subject: session.user_id,
        status: "accepted",
      }),
    ]);
  const canEditPlaylist = (playlistId: RowRefValue) =>
    anyOf([
      { $createdBy: session.author },
      policy.invitations.exists.where({
        playlist_id: playlistId,
        subject: session.user_id,
        role: "editor",
        status: "accepted",
      }),
    ]);

  policy.playlists.allowRead.where((playlist) => canReadPlaylist(playlist.id));
  policy.playlists.allowInsert.always();
  policy.playlists.allowUpdate.where({ $createdBy: session.author });
  policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
  policy.playlist_entries.allowInsert.where((entry) => canEditPlaylist(entry.playlist_id));
  policy.playlist_entries.allowUpdate.where((entry) => canEditPlaylist(entry.playlist_id));
  policy.playlist_entries.allowDelete.where((entry) => canEditPlaylist(entry.playlist_id));
  policy.invitations.allowRead.where((invite) =>
    anyOf([
      { subject: session.user_id },
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
    ]),
  );
  policy.invitations.allowInsert.where((invite) =>
    policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
  );
  policy.invitations.allowUpdate.where((invite) =>
    policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
  );
  // Recipients may perform the one-way pending → accepted transition; every
  // other invitation change (including revoke) remains owner-controlled.
  policy.invitations.allowUpdate
    .whereOld({ subject: session.user_id, status: "pending" })
    .whereNew((invite) =>
      policy.invitations.exists.where({
        id: invite.id,
        playlist_id: invite.playlist_id,
        subject: invite.subject,
        role: invite.role,
        status: "pending",
      }),
    )
    .whereNew({ subject: session.user_id, status: "accepted" });
  policy.invitations.allowDelete.where((invite) =>
    policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.author }),
  );
  policy.playback_positions.allowRead.where({ $createdBy: session.author });
  policy.playback_positions.allowInsert.always();
});
