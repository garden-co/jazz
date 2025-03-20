import { useAcceptInvite, useIsAuthenticated } from "jazz-react";
import { ID } from "jazz-tools";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { MusicaAccount, Playlist } from "./1_schema";

export function InvitePage() {
  const navigate = useNavigate();

  const isAuthenticated = useIsAuthenticated();

  useAcceptInvite({
    invitedObjectSchema: Playlist,
    onAccept: useCallback(
      async (playlistId: ID<Playlist>) => {
        const playlist = await Playlist.load(playlistId, {});

        const me = await MusicaAccount.getMe().ensureLoaded({
          resolve: {
            root: {
              playlists: true,
            },
          },
        });

        if (
          playlist &&
          !me.root.playlists.some((item) => playlist.id === item?.id)
        ) {
          me.root.playlists.push(playlist);
        }

        navigate("/playlist/" + playlistId);
      },
      [navigate],
    ),
  });

  return isAuthenticated ? (
    <p>Accepting invite....</p>
  ) : (
    <p>Please sign in to accept the invite.</p>
  );
}
