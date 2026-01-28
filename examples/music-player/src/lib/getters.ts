import { MusicaAccount, PlaylistWithTracks } from "../1_schema";

async function getCurrentIndexAndTracks() {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        activeTrack: { $onError: "catch" },
        activePlaylist: PlaylistWithTracks.resolveQuery,
      },
    },
  });

  const tracks = root.activePlaylist.tracks;
  const activeTrack = root.activeTrack;

  return {
    currentIndex: tracks.findIndex(
      (item) => item.$jazz.id === activeTrack?.$jazz.id,
    ),
    tracks: root.activePlaylist.tracks,
  };
}

export async function getNextTrack() {
  const { currentIndex, tracks } = await getCurrentIndexAndTracks();

  const nextIndex = (currentIndex + 1) % tracks.length;

  return tracks[nextIndex];
}

export async function getPrevTrack() {
  const { currentIndex, tracks } = await getCurrentIndexAndTracks();

  const previousIndex = (currentIndex - 1 + tracks.length) % tracks.length;
  return tracks[previousIndex];
}

export async function getActivePlaylistTitle(): Promise<string> {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        activePlaylist: { $onError: "catch" },
      },
    },
  });

  return root.activePlaylist?.$isLoaded
    ? root.activePlaylist.title
    : "All tracks";
}
