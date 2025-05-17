import {
  Account,
  CoList,
  CoMap,
  FileStream,
  Profile,
  coField,
} from "jazz-tools";

/** Walkthrough: Defining the data model with CoJSON
 *
 *  Here, we define our main data model of tasks, lists of tasks and projects
 *  using CoJSON's collaborative map and list types, CoMap & CoList.
 *
 *  CoMap values and CoLists items can contain:
 *  - arbitrary immutable JSON
 *  - other CoValues
 **/

export class MusicTrack extends CoMap {
  /**
   *  Attributes are defined as class properties
   *  and you can get the types from the `co` module
   *  here we are defining the title and duration for our music track
   *
   *  Tip: try to follow the coField.string defintion to discover the other available primitives!
   */
  title = coField.string;
  duration = coField.number;

  /**
   * With `coField.ref` you can define relations between your coValues.
   *
   * Attributes are required by default unless you mark them as optional.
   */
  sourceTrack = coField.optional.ref(MusicTrack);

  /**
   * In Jazz you can upload files using FileStream.
   *
   * As for any other coValue the music files we put inside FileStream
   * is available offline and end-to-end encrypted 😉
   */
  file = coField.ref(FileStream);
  waveform = coField.ref(MusicTrackWaveform);

  isExampleTrack = coField.optional.boolean;
}

export class MusicTrackWaveform extends CoMap {
  data = coField.json<number[]>();
}

/**
 * CoList is the collaborative version of Array
 *
 * They are strongly typed and accept only the type you define here
 * as "CoList.Of" argument
 */
export class ListOfTracks extends CoList.Of(coField.ref(MusicTrack)) {}

export class Playlist extends CoMap {
  title = coField.string;
  tracks = coField.ref(ListOfTracks);
}

export class ListOfPlaylists extends CoList.Of(coField.ref(Playlist)) {}

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export class MusicaAccountRoot extends CoMap {
  // The root playlist works as container for the tracks that
  // the user has uploaded
  rootPlaylist = coField.ref(Playlist);
  // Here we store the list of playlists that the user has created
  // or that has been invited to
  playlists = coField.ref(ListOfPlaylists);
  // We store the active track and playlist as coValue here
  // so when the user reloads the page can see the last played
  // track and playlist
  // You can also add the position in time if you want make it possible
  // to resume the song
  activeTrack = coField.optional.ref(MusicTrack);
  activePlaylist = coField.ref(Playlist);

  exampleDataLoaded = coField.optional.boolean;
}

export class MusicaAccount extends Account {
  profile = coField.ref(Profile);
  root = coField.ref(MusicaAccountRoot);

  /**
   *  The account migration is run on account creation and on every log-in.
   *  You can use it to set up the account root and any other initial CoValues you need.
   */
  migrate() {
    if (this.root === undefined) {
      const tracks = ListOfTracks.create([]);
      const rootPlaylist = Playlist.create({
        tracks,
        title: "",
      });

      this.root = MusicaAccountRoot.create({
        rootPlaylist,
        playlists: ListOfPlaylists.create([]),
        activeTrack: null,
        activePlaylist: rootPlaylist,
        exampleDataLoaded: false,
      });
    }
  }
}

/** Walkthrough: Continue with ./2_main.tsx */
