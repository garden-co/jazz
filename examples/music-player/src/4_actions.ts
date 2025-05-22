import { getAudioFileData } from "@/lib/audio/getAudioFileData";
import { FileStream, Group, co } from "jazz-tools";
import {
  MusicTrack,
  MusicTrackWaveform,
  MusicaAccount,
  Playlist,
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

export async function uploadMusicTracks(
  files: Iterable<File>,
  isExampleTrack: boolean = false,
) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
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
    const fileStream = await FileStream.createFromBlob(file, group);

    const musicTrack = MusicTrack.create(
      {
        file: fileStream,
        duration: data.duration,
        waveform: MusicTrackWaveform.create({ data: data.waveform }, group),
        title: file.name,
        isExampleTrack,
      },
      group,
    );

    // The newly created musicTrack can be associated to the
    // user track list using a simple push call
    root.rootPlaylist.tracks.push(musicTrack);
  }
}

export async function createNewPlaylist() {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  // Since playlists are meant to be shared we associate them
  // to a group which will contain the keys required to get
  // access to the "owned" values
  const playlistGroup = Group.create();

  const playlist = Playlist.create(
    {
      title: "New Playlist",
      tracks: co.list(MusicTrack).create([], playlistGroup),
    },
    playlistGroup,
  );

  // Again, we associate the new playlist to the
  // user by pushing it into the playlists CoList
  root.playlists.push(playlist);

  return playlist;
}

export async function addTrackToPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const alreadyAdded = playlist.tracks?.some(
    (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
  );

  if (alreadyAdded) return;

  // Check if the track has been created after the Group inheritance was introduced
  if (track._owner._type === "Group" && playlist._owner._type === "Group") {
    /**
     * Extending the track with the Playlist group in order to make the music track
     * visible to the Playlist user
     */
    const trackGroup = track._owner;
    trackGroup.extend(playlist._owner);

    playlist.tracks?.push(track);
    return;
  }
}

export async function removeTrackFromPlaylist(
  playlist: Playlist,
  track: MusicTrack,
) {
  const notAdded = !playlist.tracks?.some(
    (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
  );

  if (notAdded) return;

  if (track._owner._type === "Group" && playlist._owner._type === "Group") {
    const trackGroup = track._owner;
    await trackGroup.revokeExtend(playlist._owner);

    const index =
      playlist.tracks?.findIndex(
        (t) => t?.id === track.id || t?._refs.sourceTrack?.id === track.id,
      ) ?? -1;
    if (index > -1) {
      playlist.tracks?.splice(index, 1);
    }
    return;
  }
}

export async function updatePlaylistTitle(playlist: Playlist, title: string) {
  playlist.title = title;
}

export async function updateMusicTrackTitle(track: MusicTrack, title: string) {
  track.title = title;
}

export async function updateActivePlaylist(playlist?: Playlist) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: true,
        rootPlaylist: true,
      },
    },
  });

  root.activePlaylist = playlist ?? root.rootPlaylist;
}

export async function updateActiveTrack(track: MusicTrack) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {},
    },
  });

  root.activeTrack = track;
}

export async function onAnonymousAccountDiscarded(
  anonymousAccount: MusicaAccount,
) {
  const { root: anonymousAccountRoot } = await anonymousAccount.ensureLoaded({
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

  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
      },
    },
  });

  for (const track of anonymousAccountRoot.rootPlaylist.tracks) {
    if (track.isExampleTrack) continue;

    const trackGroup = track._owner.castAs(Group);
    trackGroup.addMember(me, "admin");

    me.root.rootPlaylist.tracks.push(track);
  }
}

export async function deletePlaylist(playlistId: string) {
  const { root } = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  const index = root.playlists.findIndex((p) => p?.id === playlistId);
  if (index > -1) {
    root.playlists.splice(index, 1);
  }
}
