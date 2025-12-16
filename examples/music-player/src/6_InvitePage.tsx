import { useAcceptInvite } from "jazz-tools/react";
import { useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { MusicaAccount, Playlist } from "./1_schema";

export function InvitePage() {
  const navigate = useNavigate();

  useAcceptInvite({
    invitedObjectSchema: Playlist,
    onAccept: useCallback(
      async (playlistId: string) => {
        const playlist = await Playlist.load(playlistId, {});

        const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
          resolve: {
            root: {
              playlists: {
                $each: {
                  $onError: "catch",
                },
              },
            },
          },
        });

        if (
          playlist &&
          !me.root.playlists.some(
            (item) => playlist.$jazz.id === item?.$jazz.id,
          )
        ) {
          me.root.playlists.$jazz.push(playlist);
        }

        navigate("/playlist/" + playlistId);
      },
      [navigate],
    ),
  });

  return <p>Accepting invite....</p>;
}
