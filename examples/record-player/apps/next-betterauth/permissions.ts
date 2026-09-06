import { schema as s } from "jazz-tools";
import type { RowRefValue } from "jazz-tools/permissions";
import { betterAuthPermissions } from "./auth-schema";
import { app } from "./schema";

const recordPlayerPermissions = s.definePermissions(
  app,
  ({ policy, allOf, anyOf, allowedTo, session }) => {
    // RecordPlayer deliberately models a shared public catalogue: every
    // authenticated listener can publish album metadata and audio tracks, and
    // everyone can browse them. A production catalog would ordinarily replace
    // these two insert rules with a label/artist membership relation.
    policy.albums.allowRead.where({});
    policy.albums.allowInsert.where({ "$createdBy.account": session.user.account });
    policy.tracks.allowRead.where({});
    policy.tracks.allowInsert.where({ "$createdBy.account": session.user.account });
    const canReadPlaylist = (playlistId: RowRefValue) =>
      anyOf([
        { "$createdBy.account": session.user.account },
        policy.invitations.exists.where({
          playlist_id: playlistId,
          // This column carries Jazz's issuer-scoped canonical session user, not a
          // provider-local Better Auth user id. It stays stable across tokens
          // and avoids conflating external account storage with row authorship.
          subject: session.user.account,
          status: "accepted",
        }),
      ]);
    const hasEditorInvitation = (playlistId: RowRefValue) =>
      policy.invitations.exists.where({
        playlist_id: playlistId,
        subject: session.user.account,
        role: "editor",
        status: "accepted",
      });
    const canEditPlaylist = (playlistId: RowRefValue) =>
      anyOf([{ "$createdBy.account": session.user.account }, hasEditorInvitation(playlistId)]);

    policy.playlists.allowRead.where((playlist) => canReadPlaylist(playlist.id));
    policy.playlists.allowInsert.always();
    policy.playlists.allowUpdate.where({ "$createdBy.account": session.user.account });
    policy.playlist_entries.allowRead.where(allowedTo.read("playlist_id"));
    policy.playlist_entries.allowInsert.where((entry) =>
      anyOf([allowedTo.update("playlist_id"), hasEditorInvitation(entry.playlist_id)]),
    );
    policy.playlist_entries.allowUpdate.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.playlist_entries.allowDelete.where((entry) => canEditPlaylist(entry.playlist_id));
    policy.invitations.allowRead.where((invite) =>
      anyOf([
        { subject: session.user.account },
        policy.playlists.exists.where({
          id: invite.playlist_id,
          "$createdBy.account": session.user.account,
        }),
      ]),
    );
    policy.invitations.allowInsert.where((invite) =>
      policy.playlists.exists.where({
        id: invite.playlist_id,
        "$createdBy.account": session.user.account,
      }),
    );
    // Recipients may perform the one-way pending → accepted transition; every
    // other invitation change (including revoke) remains owner-controlled.
    // Before, those alternatives were separate update rules; that silently
    // formed an old/new cross product. Keep them in one explicit rule.
    // The recipient's persisted pending row and accepted new row are both
    // required: chained whereNew calls replace, rather than combine, checks.
    policy.invitations.allowUpdate
      .whereOld((invite) =>
        anyOf([
          policy.playlists.exists.where({
            id: invite.playlist_id,
            "$createdBy.account": session.user.account,
          }),
          { subject: session.user.account, status: "pending" },
        ]),
      )
      .whereNew((invite) =>
        anyOf([
          policy.playlists.exists.where({
            id: invite.playlist_id,
            "$createdBy.account": session.user.account,
          }),
          allOf([
            policy.invitations.exists.where({
              id: invite.id,
              playlist_id: invite.playlist_id,
              subject: invite.subject,
              role: invite.role,
              status: "pending",
            }),
            { subject: session.user.account, status: "accepted" },
          ]),
        ]),
      );
    policy.invitations.allowDelete.where((invite) =>
      policy.playlists.exists.where({
        id: invite.playlist_id,
        "$createdBy.account": session.user.account,
      }),
    );
    policy.playback_positions.allowRead.where({ "$createdBy.account": session.user.account });
    policy.playback_positions.allowInsert.always();
    policy.playback_positions.allowUpdate
      .whereOld({ "$createdBy.account": session.user.account })
      .whereNew({ "$createdBy.account": session.user.account });
    policy.playback_positions.allowDelete.where({ "$createdBy.account": session.user.account });
  },
);

export default { ...betterAuthPermissions, ...recordPlayerPermissions };
