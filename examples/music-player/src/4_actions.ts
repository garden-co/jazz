import { getAudioFileData } from "@/lib/audio/getAudioFileData";
import { FileStream, Group } from "jazz-tools";
import {
  ListOfTracks,
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

export async function uploadMusicTracks(files: Iterable<File>) {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        rootPlaylist: {
          tracks: true,
        },
        playlists: true,
      },
    },
  });

  if (!me) return;

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
      },
      group,
    );

    // The newly created musicTrack can be associated to the
    // user track list using a simple push call
    me.root.rootPlaylist.tracks.push(musicTrack);
  }
}

export async function createNewPlaylist() {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        playlists: true,
      },
    },
  });

  if (!me) throw new Error("Current playlist not resolved");

  // Since playlists are meant to be shared we associate them
  // to a group which will contain the keys required to get
  // access to the "owned" values
  const playlistGroup = Group.create();

  const playlist = Playlist.create(
    {
      title: "New Playlist",
      tracks: ListOfTracks.create([], playlistGroup),
    },
    playlistGroup,
  );

  // Again, we associate the new playlist to the
  // user by pushing it into the playlists CoList
  me.root.playlists.push(playlist);

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

  /**
   * Since musicTracks are created as private values (see uploadMusicTracks)
   * to make them shareable as part of the playlist we are cloning them
   * and setting the playlist group as owner of the clone
   *
   * Doing this for backwards compatibility for when the Group inheritance wasn't possible
   */
  const blob = await FileStream.loadAsBlob(track._refs.file.id);
  const waveform = await MusicTrackWaveform.load(track._refs.waveform.id, {});

  if (!blob || !waveform) return;

  const trackClone = MusicTrack.create(
    {
      file: await FileStream.createFromBlob(blob, playlist._owner),
      duration: track.duration,
      waveform: MusicTrackWaveform.create(
        { data: waveform.data },
        playlist._owner,
      ),
      title: track.title,
      sourceTrack: track,
    },
    playlist._owner,
  );

  playlist.tracks?.push(trackClone);
}

export async function updatePlaylistTitle(playlist: Playlist, title: string) {
  playlist.title = title;
}

export async function updateMusicTrackTitle(track: MusicTrack, title: string) {
  track.title = title;
}

export async function updateActivePlaylist(playlist?: Playlist) {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: {
      root: {
        activePlaylist: true,
        rootPlaylist: true,
      },
    },
  });

  if (!me) return;

  me.root.activePlaylist = playlist ?? me.root.rootPlaylist;
}

export async function updateActiveTrack(track: MusicTrack) {
  const me = await MusicaAccount.getMe().ensureLoaded({
    resolve: { root: true },
  });

  if (!me) return;

  me.root.activeTrack = track;
}
