import { MusicaAccount } from "../1_schema";

export async function getNextTrack() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root._refs.activeTrack;

  const currentIndex = tracks.findIndex((item) => item?.id === activeTrack.id);

  const nextIndex = (currentIndex + 1) % tracks.length;

  return tracks[nextIndex];
}

export async function getPrevTrack() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: {
          tracks: true,
        },
      },
    },
  });

  const tracks = me.root.activePlaylist.tracks;
  const activeTrack = me.root._refs.activeTrack;

  const currentIndex = tracks.findIndex((item) => item?.id === activeTrack.id);

  const previousIndex = (currentIndex - 1 + tracks.length) % tracks.length;
  return tracks[previousIndex];
}
