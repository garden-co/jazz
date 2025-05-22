import { co, z } from "jazz-tools";

/** Walkthrough: Defining the data model with CoJSON
 *
 *  Here, we define our main data model of tasks, lists of tasks and projects
 *  using CoJSON's collaborative map and list types, CoMap & CoList.
 *
 *  CoMap values and CoLists items can contain:
 *  - arbitrary immutable JSON
 *  - other CoValues
 **/

export const MusicTrackWaveform = co.map({
  data: z.array(z.number()),
});
export type MusicTrackWaveform = co.loaded<typeof MusicTrackWaveform>;

export const MusicTrack = co.map({
  /**
   *  Attributes are defined using zod schemas
   */
  title: z.string(),
  duration: z.number(),

  /**
   * You can define relations between coValues using the other CoValue schema
   * You can mark them optional using z.optional()
   */
  waveform: MusicTrackWaveform,

  /**
   * In Jazz you can upload files using FileStream.
   *
   * As for any other coValue the music files we put inside FileStream
   * is available offline and end-to-end encrypted 😉
   */
  file: co.fileStream(),

  isExampleTrack: z.optional(z.boolean()),

  /**
   * You can use getters for recusrive relations
   */
  get sourceTrack(): z.ZodOptional<typeof MusicTrack> {
    return z.optional(MusicTrack);
  },
});
export type MusicTrack = co.loaded<typeof MusicTrack>;

export const Playlist = co.map({
  title: z.string(),
  tracks: co.list(MusicTrack), // CoList is the collaborative version of Array
});
export type Playlist = co.loaded<typeof Playlist>;
/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const MusicaAccountRoot = co.map({
  // The root playlist works as container for the tracks that
  // the user has uploaded
  rootPlaylist: Playlist,
  // Here we store the list of playlists that the user has created
  // or that has been invited to
  playlists: co.list(Playlist),
  // We store the active track and playlist as coValue here
  // so when the user reloads the page can see the last played
  // track and playlist
  // You can also add the position in time if you want make it possible
  // to resume the song
  activeTrack: z.optional(MusicTrack),
  activePlaylist: Playlist,

  exampleDataLoaded: z.optional(z.boolean()),
});
export type MusicaAccountRoot = co.loaded<typeof MusicaAccountRoot>;
export const MusicaAccount = co
  .account({
    /** the default user profile with a name */
    profile: co.profile(),
    root: MusicaAccountRoot,
  })
  .withMigration((account) => {
    /**
     *  The account migration is run on account creation and on every log-in.
     *  You can use it to set up the account root and any other initial CoValues you need.
     */
    if (account.root === undefined) {
      const tracks = co.list(MusicTrack).create([]);
      const rootPlaylist = Playlist.create({
        tracks,
        title: "",
      });

      account.root = MusicaAccountRoot.create({
        rootPlaylist,
        playlists: co.list(Playlist).create([]),
        activeTrack: undefined,
        activePlaylist: rootPlaylist,
        exampleDataLoaded: false,
      });
    }
  });
export type MusicaAccount = co.loaded<typeof MusicaAccount>;

/** Walkthrough: Continue with ./2_main.tsx */
