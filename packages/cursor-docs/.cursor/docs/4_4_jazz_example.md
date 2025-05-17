# Example app 4: A bubble tea ordering app that lets users customize drinks with different tea bases, add-ons, and delivery preferences

```typescript
import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";

export const BubbleTeaAddOnTypes = [
  "Pearl",
  "Lychee jelly",
  "Red bean",
  "Brown sugar",
  "Taro",
] as const;

export const BubbleTeaBaseTeaTypes = [
  "Black",
  "Oolong",
  "Jasmine",
  "Thai",
] as const;

/**
 * A list of Bubble Tea add-ons.
 * Provides a computed property to check for insertions.
 */
export class ListOfBubbleTeaAddOns extends CoList.Of(coField.literal(...BubbleTeaAddOnTypes)) {
  get hasChanges() {
    return Object.entries(this._raw.insertions).length > 0;
  }
}

/**
 * Represents a finalized Bubble Tea order.
 *
 * Properties:
 *  - baseTea: Selected base tea type.
 *  - addOns: Selected add-ons.
 *  - deliveryDate: Delivery date for the order.
 *  - withMilk: Indicates if the order includes milk.
 *  - instructions: Optional additional instructions.
 */
export class BubbleTeaOrder extends CoMap {
  baseTea = coField.literal(...BubbleTeaBaseTeaTypes);
  addOns = coField.ref(ListOfBubbleTeaAddOns);
  deliveryDate = coField.Date;
  withMilk = coField.boolean;
  instructions = coField.optional.string;
}

/**
 * Represents a draft (in-progress) Bubble Tea order.
 *
 * Properties:
 *  - baseTea: Optional base tea type.
 *  - addOns: Optional reference to selected add-ons.
 *  - deliveryDate: Optional delivery date.
 *  - withMilk: Optional milk preference.
 *  - instructions: Optional instructions.
 *
 * Methods:
 *  - validate: Checks that required fields are present.
 * Computed:
 *  - hasChanges: Indicates if there have been modifications.
 */
export class DraftBubbleTeaOrder extends CoMap {
  baseTea = coField.optional.literal(...BubbleTeaBaseTeaTypes);
  addOns = coField.optional.ref(ListOfBubbleTeaAddOns);
  deliveryDate = coField.optional.Date;
  withMilk = coField.optional.boolean;
  instructions = coField.optional.string;

  get hasChanges() {
    return Object.keys(this._edits).length > 1 || this.addOns?.hasChanges;
  }

  validate() {
    const errors: string[] = [];
    if (!this.baseTea) {
      errors.push("Please select your preferred base tea.");
    }
    if (!this.deliveryDate) {
      errors.push("Please select a delivery date.");
    }
    return { errors };
  }
}

/**
 * A collaborative list of finalized Bubble Tea orders.
 */
export class ListOfBubbleTeaOrders extends CoList.Of(coField.ref(BubbleTeaOrder)) {}

/**
 * Container for Bubble Tea orders.
 * Holds the draft order and the list of finalized orders.
 */
export class BubbleTeaContainer extends CoMap {
  draft = coField.ref(DraftBubbleTeaOrder);
  orders = coField.ref(ListOfBubbleTeaOrders);
}

/**
 * The top-level account root for the Bubble Tea app.
 *
 * Properties:
 *  - container: The main container that organizes the Bubble Tea orders.
 *  - version: Optional version number for migration tracking.
 */
export class BubbleTeaAccountRoot extends CoMap {
  container = coField.ref(BubbleTeaContainer);
  version = coField.optional.number;
}

/**
 * Represents a user's profile.
 *
 * Properties:
 *  - name: Required user name.
 *
 * Static method:
 *  - validate: Ensures that a non-empty name and email (if provided) are present.
 */
export class UserProfile extends Profile {
  name = coField.string;

  static validate(data: { name?: string; email?: string }) {
    const errors: string[] = [];
    if (!data.name?.trim()) {
      errors.push("Please enter a name.");
    }
    if (data.email !== undefined && !data.email.trim()) {
      errors.push("Please enter an email.");
    }
    return { errors };
  }
}

/**
 * Main account class for the Bubble Tea app.
 * Contains only the profile and root properties.
 * Handles account initialization and migrations.
 */
export class BubbleTeaAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(BubbleTeaAccountRoot);

  /**
   * The migrate method is run on account creation and login.
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
   *  - Sets up a public group (accessible by "everyone") for the user's profile.
   *  - Sets up a private group for the Bubble Tea data.
   *  - Creates a default BubbleTeaContainer with an empty draft and order list.
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

    // Create a private group for Bubble Tea data.
    const privateGroup = Group.create({ owner: this });

    // Create a default container with an empty draft order and empty list of finalized orders.
    const defaultContainer = BubbleTeaContainer.create(
      {
        draft: DraftBubbleTeaOrder.create(
          {
            addOns: ListOfBubbleTeaAddOns.create([], privateGroup),
          },
          privateGroup
        ),
        orders: ListOfBubbleTeaOrders.create([], privateGroup),
      },
      privateGroup
    );

    // Initialize the account root with version tracking.
    this.root = BubbleTeaAccountRoot.create(
      {
        container: defaultContainer,
        version: 0, // Set initial version
      },
      { owner: this }
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // Uncomment to add migrations:
  // private async migrationV1() {
  //   if (this.root) {
  //     // Example migration logic: update orders if needed.
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

Continue: 4_5_jazz_example.md
