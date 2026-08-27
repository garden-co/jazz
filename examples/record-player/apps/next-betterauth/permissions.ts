import { schema as s } from "jazz-tools";
import type { RowRefValue } from "jazz-tools/permissions";
import { betterAuthPermissions } from "./auth-schema";
import { app } from "./schema";

const recordPlayerPermissions = s.definePermissions(
  app,
  ({ policy, anyOf, allowedTo, session }) => {
    // RecordPlayer deliberately models a shared public catalogue: every
    // authenticated listener can publish album metadata and audio tracks, and
    // everyone can browse them. A production catalog would ordinarily replace
    // these two insert rules with a label/artist membership relation.
    policy.albums.allowRead.where({});
    policy.albums.allowInsert.where({ $createdBy: session.user });
    policy.tracks.allowRead.where({});
    policy.tracks.allowInsert.where({ $createdBy: session.user });
    const canReadPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.user },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          // This column carries Jazz's issuer-scoped canonical session user, not a
          // provider-local Better Auth user id. It stays stable across tokens
          // and avoids conflating external account storage with row authorship.
          subject: session.user,
          status: "accepted",
        }),
      ]);
    const canEditPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { $createdBy: session.user },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          subject: session.user,
          role: "editor",
          status: "accepted",
        }),
      ]);

    policy.playlists.allowRead.where((playlist) => canReadPlaylist(playlist.id));
    policy.playlists.allowInsert.always();
    policy.playlists.allowUpdate.where({ $createdBy: session.user });
    policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
    policy.playlist_entries.allowInsert.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowUpdate.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowDelete.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.invitations.allowRead.where((invite) =>
      anyOf([
        { subject: session.user },
        policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
      ]),
    );
    policy.invitations.allowInsert.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    policy.invitations.allowUpdate.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    // Recipients may perform the one-way pending → accepted transition; every
    // other invitation change (including revoke) remains owner-controlled.
    policy.invitations.allowUpdate
      .whereOld({ subject: session.user, status: "pending" })
      .whereNew((invite) =>
        policy.invitations.exists.where({
          id: invite.id,
          playlist_id: invite.playlist_id,
          subject: invite.subject,
          role: invite.role,
          status: "pending",
        }),
      )
      .whereNew({ subject: session.user, status: "accepted" });
    policy.invitations.allowDelete.where((invite) =>
      policy.playlists.exists.where({ id: invite.playlist_id, $createdBy: session.user }),
    );
    policy.playback_positions.allowRead.where({ $createdBy: session.user });
    policy.playback_positions.allowInsert.always();
    policy.playback_positions.allowUpdate
      .whereOld({ $createdBy: session.user })
      .whereNew({ $createdBy: session.user });
    policy.playback_positions.allowDelete.where({ $createdBy: session.user });
  },
);

export default { ...betterAuthPermissions, ...recordPlayerPermissions };
