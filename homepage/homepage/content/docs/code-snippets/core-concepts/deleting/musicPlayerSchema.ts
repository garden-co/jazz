import { co, z } from "jazz-tools";

export const MusicTrackWaveform = co.map({
  data: z.array(z.number()),
});
export type MusicTrackWaveform = co.loaded<typeof MusicTrackWaveform>;

export const MusicTrack = co.map({
  title: z.string(),
  duration: z.number(),
  waveform: MusicTrackWaveform,
  file: co.fileStream(),

  isExampleTrack: z.optional(z.boolean()),
});
export type MusicTrack = co.loaded<typeof MusicTrack>;

export const Playlist = co.map({
  title: z.string(),
  tracks: co.list(MusicTrack), // CoList is the collaborative version of Array
});
export type Playlist = co.loaded<typeof Playlist>;

export const MusicaAccountRoot = co
  .map({
    rootPlaylist: Playlist,
    playlists: co.list(Playlist),
    activeTrack: co.optional(MusicTrack),
    activePlaylist: Playlist,

    exampleDataLoaded: z.optional(z.boolean()),
    accountSetupCompleted: z.optional(z.boolean()),
  })
  .withPermissions({ onInlineCreate: "newGroup" })
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

