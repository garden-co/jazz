---

import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";

/**
 * Represents a main data item in the app’s domain.
 *
 * Properties:
 *  - name: Required field identifying the item.
 *  - metadata_field: Optional metadata (string).
 *  - container: Reference to a parent Container.
 *  - deleted: Soft delete flag for archiving/removing without permanent deletion.
 */
export class MainItem extends CoMap {
  /** A required, identifying name. */
  name = coField.string;

  /** An optional string field for metadata. */
  metadata_field = coField.optional.string;

  /** Reference to the parent container. */
  container = coField.ref(Container);

  /** Soft-delete flag: if true, treat this item as removed. */
  deleted = coField.boolean;
}

/**
 * A list/array of MainItem references.
 * Provides real-time collaboration features (insertion, removal, ordering).
 */
export class MainItemList extends CoList.Of(coField.ref(MainItem)) {}

/**
 * A container/organizational structure for grouping MainItem objects.
 *
 * Properties:
 *  - name: A human-friendly name for the container.
 *  - items: A CoList of MainItem references.
 */
export class Container extends CoMap {
  /** Human-friendly name for this container. */
  name = coField.string;

  /** A list of MainItems held by this container. */
  items = coField.ref(MainItemList);
}

/**
 * The top-level structure in the user’s account, representing all stored data.
 *
 * Properties:
 *  - container: The default or root container for MainItems.
 *  - version: An optional version number for supporting migrations.
 */
export class AccountRoot extends CoMap {
  /** A single container to hold or organize items. */
  container = coField.ref(Container);

  /** Tracks schema version for migrations. */
  version = coField.optional.number;
}

/**
 * Represents a user’s profile data.
 *
 * Properties:
 *  - email: Required email field for identification/contact.
 *
 * Static method:
 *  - validate: Enforces that both name and email are provided.
 */
export class UserProfile extends Profile {
  /** Required user email. */
  email = coField.string;

  /**
   * Validate user profile data, ensuring both "name" and "email" exist and are non-empty.
   */
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
 * The main Account class that holds the user’s data (AccountRoot) and profile.
 * Handles initial migrations (setting up default Container, etc.) and can be extended
 * to run future schema migrations.
 */
export class JazzAccount extends Account {
  /** Reference to the user’s profile. */
  profile = coField.ref(UserProfile);

  /** Reference to the account root data (container, version, etc.). */
  root = coField.ref(AccountRoot);

  /**
   * Migrate is run on creation and each login. If there is no root, creates initial data.
   * Otherwise, you can add version-based migrations (below).
   */
  async migrate(creationProps?: { name: string; other?: Record<string, unknown> }) {
    if (!this._refs.root && creationProps) {
      await this.initialMigration(creationProps);
      return;
    }

    // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
    // uncomment this to add migrations
    // Check the current version and run subsequent migrations
    // const currentVersion = this.root?.version || 0;
    // if (currentVersion < 1) {
    //   await this.migrationV1();
    // }
    // Add more version checks and migrations as needed
    // if (currentVersion < 2) {
    //   await this.migrationV2();
    // }
  }

  /**
   * Executes the initial migration logic when the account is first created:
   *  - Validates the user’s profile data (name, email).
   *  - Sets up a public group (readable by "everyone") for the user’s profile.
   *  - Sets up a private group to own private resources.
   *  - Creates a default Container with a single MainItem.
   */
  private async initialMigration(
    creationProps: { name: string; other?: Record<string, unknown> }
  ) {
    const { name, other } = creationProps;

    // Validate profile data
    const profileErrors = UserProfile.validate({ name, ...other });
    if (profileErrors.errors.length > 0) {
      throw new Error(
        "Invalid profile data: " + profileErrors.errors.join(", "),
      );
    }

    // Create a public group for the profile
    const publicGroup = Group.create({ owner: this });
    publicGroup.addMember("everyone", "reader");

    // Create the user profile with validated data
    this.profile = UserProfile.create(
      {
        name,
        ...other,
      },
      { owner: publicGroup },
    );

    // Create a private group for data that should not be publicly readable
    const privateGroup = Group.create({ owner: this });

    // Create a default container with one default item
    const defaultContainer = Container.create(
      {
        name: this.profile?.name
          ? \`\${this.profile.name}'s items\`
          : "Your items",
        items: MainItemList.create(
          [
            MainItem.create({ name: "Default item" }, privateGroup),
          ],
          privateGroup,
        ),
      },
      privateGroup,
    );

    // Initialize the account root with version tracking
    this.root = AccountRoot.create(
      {
        container: defaultContainer,
        version: 0, // Start at version 0
      },
      { owner: this },
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // uncomment this to add migrations
  // private async migrationV1() {
  //   Example migration logic:
  //   if (this.root) {
  //     // e.g., add a new field to all items
  //     // for (const container of this.root.containers || []) {
  //     //   for (const item of container.items || []) {
  //     //     item.newField = "default value";
  //     //   }
  //     // }
  //     this.root.version = 1;
  //   }
  // }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // uncomment this to add migrations
  // private async migrationV2() {
  //   if (this.root) {
  //     // Future migration logic goes here
  //     this.root.version = 2;
  //   }
  // }
}

---

Continue: 3_jazz_rules.md
