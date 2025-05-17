# Example app 2: A feature-rich music player app that allows users to manage playlists, store tracks, and visualize audio waveforms

```typescript
export class MusicTrack extends CoMap {
  title = coField.string;
  duration = coField.number;
  sourceTrack = coField.optional.ref(MusicTrack);
  file = coField.ref(FileStream);
  waveform = coField.ref(MusicTrackWaveform);
  container = coField.ref(Playlist);
  deleted = coField.boolean;
}

/**
 * Represents waveform data for a music track.
 *
 * Properties:
 *  - data: A JSON array of numbers representing the waveform.
 */
export class MusicTrackWaveform extends CoMap {
  data = coField.json<number[]>();
}

/**
 * A collaborative list of MusicTrack references.
 */
export class MusicTrackList extends CoList.Of(coField.ref(MusicTrack)) {}

/**
 * Acts as a container for music tracks.
 *
 * Properties:
 *  - name: The name of the playlist.
 *  - items: A list of MusicTracks in this playlist.
 */
export class Playlist extends CoMap {
  name = coField.string;
  items = coField.ref(MusicTrackList);
}

/**
 * The top-level account root for the music app.
 *
 * Properties:
 *  - container: The main playlist (acting as the container for music tracks).
 *  - version: Optional version number for migrations.
 */
export class MusicAccountRoot extends CoMap {
  container = coField.ref(Playlist);
  version = coField.optional.number;
}

/**
 * Represents a user's profile.
 *
 * Properties:
 *  - name: The required user name.
 *
 * Static method:
 *  - validate: Ensures that a non-empty name and email are provided.
 */
export class UserProfile extends Profile {
  name = coField.string;

  static validate(data: { name?: string; email?: string }) {
    const errors: string[] = [];
    if (!data.name?.trim()) {
      errors.push("Please enter a name.");
    }
    if (!data.email?.trim()) {
      errors.push("Please enter an email.");
    }
    return { errors };
  }
}

/**
 * The main Account class for the music app.
 * Contains only the profile and root properties.
 * Handles data initialization and migrations.
 */
export class MusicAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(MusicAccountRoot);

  /**
   * Migrate is run on account creation and each login.
   * If the root is not initialized, run initial migration.
   * Otherwise, version-based migrations can be added.
   */
  async migrate(creationProps?: { name: string; other?: Record<string, unknown> }) {
    if (!this._refs.root && creationProps) {
      await this.initialMigration(creationProps);
      return;
    }

    // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
    // Uncomment to add migrations:
    // const currentVersion = this.root?.version || 0;
    // if (currentVersion < 1) {
    //   await this.migrationV1();
    // }
    // if (currentVersion < 2) {
    //   await this.migrationV2();
    // }
  }

  /**
   * Executes initial migration when the account is first created:
   *  - Validates the user's profile data (name, email).
   *  - Sets up a public group (with "everyone" as reader) for the profile.
   *  - Creates a default Playlist with an empty MusicTrackList.
   *  - Initializes the account root with version 0.
   */
  private async initialMigration(creationProps: { name: string; other?: Record<string, unknown> }) {
    const { name, other } = creationProps;
    const profileErrors = UserProfile.validate({ name, ...other });
    if (profileErrors.errors.length > 0) {
      throw new Error("Invalid profile data: " + profileErrors.errors.join(", "));
    }

    // Create a public group for the profile.
    const publicGroup = Group.create({ owner: this });
    publicGroup.addMember("everyone", "reader");

    // Create the user profile with validated data.
    this.profile = UserProfile.create(
      { name, ...other },
      { owner: publicGroup }
    );

    // Create a private group for the user's music data.
    const privateGroup = Group.create({ owner: this });

    // Create a default Playlist as the main container.
    const defaultPlaylist = Playlist.create(
      {
        name: this.profile.name + "'s playlist",
        items: MusicTrackList.create([], privateGroup),
      },
      privateGroup
    );

    // Initialize the account root with version tracking.
    this.root = MusicAccountRoot.create(
      {
        container: defaultPlaylist,
        version: 0, // Set initial version
      },
      { owner: this }
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // Uncomment to add migrations:
  // private async migrationV1() {
  //   if (this.root) {
  //     // Example migration logic: add a new field to all music tracks.
  //     // for (const track of this.root.container.items || []) {
  //     //   track.newField = "default value";
  //     // }
  //     this.root.version = 1;
  //   }
  // }

  // private async migrationV2() {
  //   if (this.root) {
  //     // Future migration logic goes here.
  //     this.root.version = 2;
  //   }
  // }
}
```

---

Continue: 4_3_jazz_example.md
