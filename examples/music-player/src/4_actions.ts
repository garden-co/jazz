import { getAudioFileData } from "@/lib/audio/getAudioFileData";
import { Group } from "jazz-tools";
import { MusicTrack, MusicaAccount, Playlist } from "./1_schema";

/**
 * Walkthrough: Actions
 *
 * With Jazz is very simple to update the state, you
 * just mutate the values and we take care of triggering
 * the updates and sync  and persist the values you change.
 *
 * We have grouped the complex updates here in an actions file
 * just to keep them separated from the components.
 *
 * Jazz is very unopinionated in this sense and you can adopt the
 * pattern that best fits your app.
 */

export async function uploadMusicTracks(
  files: Iterable<File>,
  isExampleTrack: boolean = false,
) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  for (const file of files) {
    // The ownership object defines the user that owns the created coValues
    // We are creating a group for each CoValue in order to be able to share them via Playlist
    const group = Group.create();

    const data = await getAudioFileData(file);

    // We transform the file blob into a FileStream
    // making it a collaborative value that is encrypted, easy
    // to share across devices and users and available offline!
    const fileStream = await MusicTrack.shape.file.createFromBlob(file, group);

    const track = MusicTrack.create(
      {
        file: fileStream,
        duration: data.duration,
        waveform: { data: data.waveform },
        title: file.name,
        isExampleTrack,
      },
      group,
    );

    // We create a new music track and add it to the root playlist
    root.rootPlaylist.tracks.$jazz.push(track);
  }
}

export async function createNewPlaylist(title: string = "New Playlist") {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  const playlist = Playlist.create({
    title,
    tracks: [],
  });

  // We associate the new playlist to the
  // user by pushing it into the playlists CoList
  root.playlists.$jazz.push(playlist);

  return playlist;
}

export async function addTrackToPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const { tracks } = await playlist.$jazz.ensureLoaded({
    resolve: {
      tracks: { $each: true },
    },
  });

  const isPartOfThePlaylist = tracks.some((t) => t.$jazz.id === track.$jazz.id);
  if (isPartOfThePlaylist) return;

  track.$jazz.owner.addMember(playlist.$jazz.owner);
  tracks.$jazz.push(track);
}

export async function removeTrackFromPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const { tracks } = await playlist.$jazz.ensureLoaded({
    resolve: {
      tracks: { $each: true },
    },
  });

  const isPartOfThePlaylist = tracks.some((t) => t.$jazz.id === track.$jazz.id);

  if (!isPartOfThePlaylist) return;

  // We remove the track before removing the access
  // because the removeMember might remove our own access
  tracks.$jazz.remove((t) => t.$jazz.id === track.$jazz.id);

  track.$jazz.owner.removeMember(playlist.$jazz.owner);
}

export async function removeTrackFromAllPlaylists(track: MusicTrack) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
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

  const playlists = root.playlists;

  // @ts-expect-error - https://github.com/microsoft/TypeScript/issues/62621
  for (const playlist of playlists) {
    if (!playlist.$isLoaded) continue;

    removeTrackFromPlaylist(playlist, track);
  }
}

export async function updatePlaylistTitle(playlist: Playlist, title: string) {
  playlist.$jazz.set("title", title);
}

export async function updateMusicTrackTitle(track: MusicTrack, title: string) {
  track.$jazz.set("title", title);
}

export async function updateActivePlaylist(playlist?: Playlist) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: true,
      },
    },
  });

  root.$jazz.set("activePlaylist", playlist ?? root.rootPlaylist);
}

export async function updateActiveTrack(track: MusicTrack) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {},
    },
  });

  root.$jazz.set("activeTrack", track);
}

export async function onAnonymousAccountDiscarded(
  anonymousAccount: MusicaAccount,
) {
  const { root: anonymousAccountRoot } =
    await anonymousAccount.$jazz.ensureLoaded({
      resolve: {
        root: {
          rootPlaylist: {
            tracks: {
              $each: true,
            },
          },
        },
      },
    });

  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  // @ts-expect-error - https://github.com/microsoft/TypeScript/issues/62621
  for (const track of anonymousAccountRoot.rootPlaylist.tracks) {
    if (track.isExampleTrack) continue;

    const trackGroup = track.$jazz.owner;
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.$jazz.push(track);
  }
}

export async function deletePlaylist(playlistId: string) {
  const { root } = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  const index = root.playlists.findIndex((p) => p?.$jazz.id === playlistId);
  if (index > -1) {
    root.playlists?.$jazz.splice(index, 1);
  }
}
