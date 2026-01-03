import { getAudioFileData } from "@/lib/audio/getAudioFileData";
import { Group, deleteCoValues } from "jazz-tools";
import {
  MusicTrack,
  MusicaAccount,
  Playlist,
  PlaylistWithTracks,
  MusicaAccountWithPlaylists,
} from "./1_schema";

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
export async function createMusicTrackFromFile(
  file: File,
  isExampleTrack: boolean = false,
) {
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

  return track;
}

export async function uploadMusicTracks(
  playlist: PlaylistWithTracks,
  files: Iterable<File>,
) {
  for (const file of files) {
    const track = await createMusicTrackFromFile(file);

    // We create a new music track and add it to the root playlist
    playlist.tracks.$jazz.push(track);
  }
}

export async function createNewPlaylist(
  me: MusicaAccountWithPlaylists,
  title: string = "New Playlist",
) {
  const playlist = Playlist.create({
    title,
    tracks: [],
  });

  // We associate the new playlist to the
  // user by pushing it into the playlists CoList
  me.root.playlists.$jazz.push(playlist);

  return playlist;
}

export async function addTrackToPlaylist(
  playlist: PlaylistWithTracks,
  track: MusicTrack,
) {
  const isPartOfThePlaylist = playlist.tracks.some(
    (t) => t.$jazz.id === track.$jazz.id,
  );
  if (isPartOfThePlaylist) return;

  track.$jazz.owner.addMember(playlist.$jazz.owner);
  playlist.tracks.$jazz.push(track);
}

export async function removeTrackFromPlaylist(
  playlist: PlaylistWithTracks,
  track: MusicTrack,
) {
  const isPartOfThePlaylist = playlist.tracks.some(
    (t) => t.$jazz.id === track.$jazz.id,
  );

  if (!isPartOfThePlaylist) return;

  // We remove the track before removing the access
  // because the removeMember might remove our own access
  playlist.tracks.$jazz.remove((t) => t.$jazz.id === track.$jazz.id);

  track.$jazz.owner.removeMember(playlist.$jazz.owner);
}

export async function deleteMusicTrack(track: MusicTrack) {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: PlaylistWithTracks.resolveQuery,
        playlists: {
          $each: {
            $onError: "catch",
            ...PlaylistWithTracks.resolveQuery,
          },
        },
      },
    },
  });

  const playlists = me.root.playlists;

  for (const playlist of playlists.values()) {
    if (!playlist.$isLoaded) continue;

    removeTrackFromPlaylist(playlist, track);
  }

  removeTrackFromPlaylist(me.root.rootPlaylist, track);

  if (me.canAdmin(track)) {
    await deleteCoValues(MusicTrack, track.$jazz.id, {
      resolve: {
        file: true,
        waveform: true,
      },
    });
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
      root: true,
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
          rootPlaylist: PlaylistWithTracks.resolveQuery,
        },
      },
    });

  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: PlaylistWithTracks.resolveQuery,
      },
    },
  });

  for (const track of anonymousAccountRoot.rootPlaylist.tracks.values()) {
    if (track.isExampleTrack) continue;

    const trackGroup = track.$jazz.owner;
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.$jazz.push(track);
  }
}

export async function deletePlaylist(playlistId: string) {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        playlists: true,
        activePlaylist: { $onError: "catch" },
        rootPlaylist: PlaylistWithTracks.resolveQuery,
        activeTrack: { $onError: "catch" },
      },
    },
  });

  const root = me.root;

  const index = root.playlists.findIndex((p) => p.$jazz.id === playlistId);
  if (index > -1) {
    root.playlists?.$jazz.splice(index, 1);
  }

  if (root.activePlaylist?.$jazz.id === playlistId) {
    root.$jazz.set("activePlaylist", root.rootPlaylist);

    if (
      !root.rootPlaylist.tracks.some(
        (t) => t.$jazz.id === root.activeTrack?.$jazz.id,
      )
    ) {
      root.$jazz.set("activeTrack", undefined);
    }
  }

  const playlist = await Playlist.load(playlistId);

  if (playlist.$isLoaded && me.canAdmin(playlist)) {
    await deleteCoValues(Playlist, playlist.$jazz.id);
  }
}

export async function deleteMyMusicPlayerAccount() {
  const me = await MusicaAccount.getMe().$jazz.ensureLoaded({
    resolve: {
      root: {
        playlists: {
          $each: { $onError: "catch" },
        },
      },
    },
  });

  // Delete all playlists referenced in the user's root. (This may include invited playlists.)
  for (const playlist of me.root.playlists.values()) {
    if (!playlist.$isLoaded) continue;
    if (me.canAdmin(playlist)) {
      await deleteCoValues(Playlist, playlist.$jazz.id);
    } else {
      playlist.$jazz.owner.removeMember(me);
    }
  }

  await deleteCoValues(MusicaAccount, me.$jazz.id, {
    resolve: {
      profile: {
        avatar: {
          $each: true,
        },
      },
      root: {
        rootPlaylist: {
          tracks: {
            $each: {
              file: true,
              waveform: true,
            },
          },
        },
        playlists: true,
      },
    },
  });
}
