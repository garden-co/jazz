# Example app 1: A secure and organized password manager app that allows users to store, manage, and categorize their credentials in folders

```typescript
import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";

/**
 * Represents a password item in the Password Manager.
 *
 * Properties:
 *  - name: The required name identifying the password item.
 *  - username: Optional username.
 *  - username_input_selector: Optional selector for the username input field.
 *  - password: The required password.
 *  - password_input_selector: Optional selector for the password input.
 *  - uri: Optional URI associated with the item.
 *  - folder: Reference to the parent Folder.
 *  - deleted: Soft delete flag.
 */
export class PasswordItem extends CoMap {
  name = coField.string;
  username = coField.optional.string;
  username_input_selector = coField.optional.string;
  password = coField.string;
  password_input_selector = coField.optional.string;
  uri = coField.optional.string;
  folder = coField.ref(Folder);
  deleted = coField.boolean;
}

/**
 * A list of PasswordItem references.
 */
export class PasswordList extends CoList.Of(coField.ref(PasswordItem)) {}

/**
 * Represents a folder that groups password items.
 *
 * Properties:
 *  - name: The folder's name.
 *  - items: A list of PasswordItems contained in the folder.
 */
export class Folder extends CoMap {
  name = coField.string;
  items = coField.ref(PasswordList);
}

/**
 * A list of Folder references.
 */
export class FolderList extends CoList.Of(coField.ref(Folder)) {}

/**
 * Top-level container for the Password Manager.
 * This container holds the main entities of the app.
 *
 * Properties:
 *  - folders: A list of Folder entities.
 */
export class Container extends CoMap {
  folders = coField.ref(FolderList);
}

/**
 * The account root holds all user data.
 *
 * Properties:
 *  - container: The main container that organizes the app’s data.
 *  - version: An optional version number used for migrations.
 */
export class PasswordManagerAccountRoot extends CoMap {
  container = coField.ref(Container);
  version = coField.optional.number;
}

/**
 * Represents the user's profile.
 *
 * Properties:
 *  - name: The required user name.
 *
 * Static method:
 *  - validate: Ensures that a non-empty name is provided.
 */
export class UserProfile extends Profile {
  name = coField.string;

  static validate(data: { name?: string; email?: string }) {
    const errors: string[] = [];
    if (!data.name?.trim()) {
      errors.push("Please enter a name.");
    }
    // Note: In this schema, only 'name' is required.
    return { errors };
  }
}

/**
 * Main account class for the Password Manager.
 * Contains only the profile and root properties.
 * Handles data initialization and migrations.
 */
export class PasswordManagerAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(PasswordManagerAccountRoot);

  /**
   * The migrate method is called on account creation and login.
   * If the root is not initialized, it runs the initial migration.
   * Otherwise, you can add version-based migrations as needed.
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
   * Executes the initial migration logic when the account is first created.
   *  - Validates the user's profile data.
   *  - Sets up a public group for the profile (accessible by "everyone").
   *  - Sets up a private group for private resources.
   *  - Creates a default Container with a default Folder and a default PasswordItem.
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
      {
        name,
        ...other,
      },
      { owner: publicGroup }
    );

    // Create a private group for private data.
    const privateGroup = Group.create({ owner: this });

    // Create a default Folder with one default PasswordItem.
    const defaultFolder = Folder.create(
      {
        name: "Default",
        items: PasswordList.create(
          [
            PasswordItem.create(
              {
                name: "Gmail",
                username: "user@gmail.com",
                password: "password123",
                uri: "https://gmail.com",
                // The folder reference will be set after defaultFolder creation.
                folder: null as any,
                deleted: false,
              },
              privateGroup
            ),
          ],
          privateGroup
        ),
      },
      privateGroup
    );
    // Set the folder reference for the default PasswordItem.
    defaultFolder.items[0].folder = defaultFolder;

    // Create a default container that holds the FolderList.
    const defaultContainer = Container.create(
      {
        folders: FolderList.create([defaultFolder], privateGroup),
      },
      privateGroup
    );

    // Initialize the account root with version tracking.
    this.root = PasswordManagerAccountRoot.create(
      {
        container: defaultContainer,
        version: 0, // Set initial version
      },
      { owner: this }
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // Uncomment the following methods to add migrations:

  // private async migrationV1() {
  //   if (this.root) {
  //     // Example migration logic: add a new field to all password items.
  //     // for (const folder of this.root.container.folders || []) {
  //     //   for (const item of folder.items || []) {
  //     //     item.newField = "default value";
  //     //   }
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

Continue: 4_2_jazz_example.md
