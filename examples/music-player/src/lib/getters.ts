import { MusicaAccount } from "../1_schema";

export async function getNextTrack() {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root.$jazz.refs.activeTrack;

  const currentIndex = tracks.findIndex(
    (item) => item?.$jazz.id === activeTrack?.id,
  );

  const nextIndex = (currentIndex + 1) % tracks.length;

  return tracks[nextIndex];
}

export async function getPrevTrack() {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root.$jazz.refs.activeTrack;

  const currentIndex = tracks.findIndex(
    (item) => item?.$jazz.id === activeTrack?.id,
  );

  const previousIndex = (currentIndex - 1 + tracks.length) % tracks.length;
  return tracks[previousIndex];
}
