# Example app 3: A social pet app where users can share pet photos, react with fun emojis, and organize posts in a collaborative feed

```typescript
import { Account, CoFeed, CoList, CoMap, Group, ImageDefinition, Profile, coField } from "jazz-tools";

export const ReactionTypes = [
  "aww",
  "love",
  "haha",
  "wow",
  "tiny",
  "chonkers",
] as const;

export type ReactionType = (typeof ReactionTypes)[number];

/**
 * Represents an append-only feed of reactions for a pet post.
 */
export class PetReactions extends CoFeed.Of(coField.json<ReactionType>()) {}

/**
 * Represents a pet post.
 *
 * Properties:
 *  - name: The title or caption for the pet post.
 *  - image: A reference to an ImageDefinition containing the pet's image.
 *  - reactions: A feed of reactions (of type ReactionType) for the post.
 */
export class PetPost extends CoMap {
  name = coField.string;
  image = coField.ref(ImageDefinition);
  reactions = coField.ref(PetReactions);
}

/**
 * A collaborative list of PetPost references.
 */
export class ListOfPosts extends CoList.Of(coField.ref(PetPost)) {}

/**
 * Container for the pet posts.
 *
 * This container acts as the main organizational structure holding the posts.
 *
 * Properties:
 *  - posts: A list of pet posts.
 */
export class PetContainer extends CoMap {
  posts = coField.ref(ListOfPosts);
}

/**
 * The top-level account root for the pet app.
 *
 * Properties:
 *  - container: The main container that organizes pet posts.
 *  - version: An optional version number for supporting migrations.
 */
export class PetAccountRoot extends CoMap {
  container = coField.ref(PetContainer);
  version = coField.optional.number;
}

/**
 * Represents a user’s profile.
 *
 * Properties:
 *  - name: The required user name.
 *
 * Static method:
 *  - validate: Ensures that both "name" and "email" (if provided) are non-empty.
 */
export class UserProfile extends Profile {
  name = coField.string;

  static validate(data: { name?: string; email?: string }) {
    const errors: string[] = [];
    if (!data.name?.trim()) {
      errors.push("Please enter a name.");
    }
    if (data.email !== undefined && !data.email?.trim()) {
      errors.push("Please enter an email.");
    }
    return { errors };
  }
}

/**
 * Main account class for the pet app.
 *
 * Contains only the profile and root properties, and handles account initialization
 * and migrations.
 */
export class PetAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(PetAccountRoot);

  /**
   * Migrate is run on account creation and on every log-in.
   * If the root is not initialized, it runs the initial migration.
   * Otherwise, version-based migrations can be added.
   */
  async migrate(creationProps?: { name: string; other?: Record<string, unknown> }) {
    if (!this._refs.root && creationProps) {
      await this.initialMigration(creationProps);
      return;
    }

    // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
    // Uncomment the following lines to add migrations:
    // const currentVersion = this.root?.version || 0;
    // if (currentVersion < 1) {
    //   await this.migrationV1();
    // }
    // if (currentVersion < 2) {
    //   await this.migrationV2();
    // }
  }

  /**
   * Executes the initial migration logic when the account is first created:
   *  - Validates the user's profile data (name, email).
   *  - Sets up a public group (accessible by "everyone") for the user’s profile.
   *  - Sets up a private group for the user's pet posts.
   *  - Creates a default container with an empty list of posts.
   *  - Initializes the account root with version 0.
   */
  private async initialMigration(creationProps: { name: string; other?: Record<string, unknown> }) {
    const { name, other } = creationProps;
    const profileErrors = UserProfile.validate({ name, ...other });
    if (profileErrors.errors.length > 0) {
      throw new Error("Invalid profile data: " + profileErrors.errors.join(", "));
    }

    // Create a public group for the user profile.
    const publicGroup = Group.create({ owner: this });
    publicGroup.addMember("everyone", "reader");

    // Create the user profile with validated data.
    this.profile = UserProfile.create(
      { name, ...other },
      { owner: publicGroup }
    );

    // Create a private group for pet data.
    const privateGroup = Group.create({ owner: this });

    // Create a default container holding an empty list of posts.
    const defaultContainer = PetContainer.create(
      { posts: ListOfPosts.create([], privateGroup) },
      privateGroup
    );

    // Initialize the account root with version tracking.
    this.root = PetAccountRoot.create(
      { container: defaultContainer, version: 0 },
      { owner: this }
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // Uncomment to add migrations:
  // private async migrationV1() {
  //   if (this.root) {
  //     // Example migration logic: update pet posts if needed.
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

Continue: 4_4_jazz_example.md
