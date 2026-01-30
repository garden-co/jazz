import {
  co,
  z,
  setDefaultSchemaPermissions,
  setDefaultValidationMode,
} from "jazz-tools";

setDefaultValidationMode("strict");

setDefaultSchemaPermissions({
  onInlineCreate: "sameAsContainer",
});

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
});
export type MusicTrack = co.loaded<typeof MusicTrack>;

export const Playlist = co.map({
  title: z.string(),
  tracks: co.list(MusicTrack), // CoList is the collaborative version of Array
});
export type Playlist = co.loaded<typeof Playlist>;

export const PlaylistWithTracks = Playlist.resolved({
  tracks: { $each: true },
});
export type PlaylistWithTracks = co.loaded<typeof PlaylistWithTracks>;

/** The account root is an app-specific per-user private `CoMap`
 *  where you can store top-level objects for that user */
export const MusicaAccountRoot = co
  .map({
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
    activeTrack: co.optional(MusicTrack),
    activePlaylist: Playlist,

    exampleDataLoaded: z.optional(z.boolean()),
    accountSetupCompleted: z.optional(z.boolean()),
  })
  .withPermissions({ onInlineCreate: "newGroup" })
  .resolved({
    activeTrack: { $onError: "catch" },
    activePlaylist: { $onError: "catch" },
  });
export type MusicaAccountRoot = co.loaded<typeof MusicaAccountRoot>;

export const MusicaAccountProfile = co
  .profile({
    avatar: co.optional(co.image()),
  })
  .withPermissions({
    onCreate(group) {
      group.addMember("everyone", "reader");
    },
  });
export type MusicaAccountProfile = co.loaded<typeof MusicaAccountProfile>;

export const MusicaAccount = co
  .account({
    /** the default user profile with a name */
    profile: MusicaAccountProfile,
    root: MusicaAccountRoot,
  })
  .withMigration(async (account) => {
    /**
     *  The account migration is run on account creation and on every log-in.
     *  You can use it to set up the account root and any other initial CoValues you need.
     */
    if (!account.$jazz.has("root")) {
      const rootPlaylist = Playlist.create({
        tracks: [],
        title: "",
      });

      account.$jazz.set("root", {
        rootPlaylist,
        playlists: [],
        activeTrack: undefined,
        activePlaylist: rootPlaylist,
        exampleDataLoaded: false,
      });
    }

    if (!account.$jazz.has("profile")) {
      account.$jazz.set("profile", {
        name: "",
      });
    }
  })
  .resolved({
    profile: true,
    root: MusicaAccountRoot.resolveQuery,
  });
export type MusicaAccount = co.loaded<typeof MusicaAccount>;

export const MusicaAccountWithPlaylists = MusicaAccount.resolved({
  root: {
    playlists: {
      $each: { $onError: "catch", ...PlaylistWithTracks.resolveQuery },
    },
  },
});
export type MusicaAccountWithPlaylists = co.loaded<
  typeof MusicaAccountWithPlaylists
>;
/** Walkthrough: Continue with ./2_main.tsx */
