# Example app 5: An employee onboarding app that streamlines the hiring process through structured steps, including initial data collection, document uploads, and final approvals

```typescript
import { Account, CoList, CoMap, Group, ImageDefinition, Profile, coField } from "jazz-tools";

type Steps = "initial" | "upload" | "final";

interface Step {
  type: Steps;
  prevStep: ReturnType<typeof coField.ref> | undefined;
  done: boolean;
  isCurrentStep(): boolean;
}

/**
 * Represents the initial onboarding step.
 *
 * Properties:
 *  - type: Always "initial".
 *  - ssn: Optional Social Security Number.
 *  - address: Optional address.
 *  - done: Indicates if this step is completed.
 *  - prevStep: Not applicable for the initial step.
 */
export class CoInitialStep extends CoMap implements Step {
  type = coField.literal("initial");
  ssn? = coField.string;
  address? = coField.string;
  done = coField.boolean;
  prevStep = co.null;
  isCurrentStep() {
    return !this.done;
  }
}

/**
 * Represents the document upload step.
 *
 * Properties:
 *  - type: Always "upload".
 *  - prevStep: Reference to the completed initial step.
 *  - photo: Optional reference to an image (e.g. document photo).
 *  - done: Indicates if this step is completed.
 */
export class CoDocUploadStep extends CoMap implements Step {
  type = coField.literal("upload");
  prevStep = coField.ref(CoInitialStep);
  photo = coField.ref(ImageDefinition, { optional: true });
  done = coField.boolean;
  isCurrentStep() {
    return !!(this.prevStep?.done && !this.done);
  }
}

/**
 * Represents the final onboarding step.
 *
 * Properties:
 *  - type: Always "final".
 *  - prevStep: Reference to the completed document upload step.
 *  - done: Indicates if this step is completed.
 */
export class CoFinalStep extends CoMap implements Step {
  type = coField.literal("final");
  prevStep = coField.ref(CoDocUploadStep);
  done = coField.boolean;
  isCurrentStep() {
    return !!(this.prevStep?.done && !this.done);
  }
}

/**
 * Represents an employee undergoing the onboarding process.
 *
 * Properties:
 *  - name: The employee's name.
 *  - deleted: Optional soft-delete flag.
 *  - initialStep: Reference to the initial step.
 *  - docUploadStep: Reference to the document upload step.
 *  - finalStep: Reference to the final step.
 */
export class CoEmployee extends CoMap {
  name = coField.string;
  deleted? = coField.boolean;
  initialStep = coField.ref(CoInitialStep);
  docUploadStep = coField.ref(CoDocUploadStep);
  finalStep = coField.ref(CoFinalStep);
}

/**
 * A collaborative list of employee references.
 */
export class EmployeeList extends CoList.Of(coField.ref(CoEmployee)) {}

/**
 * The top-level account root for the HR app.
 *
 * Properties:
 *  - employees: A list of employees.
 *  - version: Optional version number for migrations.
 */
export class HRAccountRoot extends CoMap {
  employees = coField.ref(EmployeeList);
  version = coField.optional.number;
}

/**
 * Represents a user's profile.
 *
 * Properties:
 *  - name: The required user name.
 *
 * Static method:
 *  - validate: Ensures that a non-empty name (and email, if provided) is present.
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
 * The main account class for the HR app.
 * Contains only the profile and root properties.
 * Handles account initialization and migrations.
 */
export class HRAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(HRAccountRoot);

  /**
   * Migrate is run on account creation and on every log-in.
   * If the account root is not initialized, it runs the initial migration.
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
   *  - Validates the user's profile data.
   *  - Sets up a public group (accessible by "everyone") for the user profile.
   *  - Creates a default HRAccountRoot with an empty employee list.
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
    this.profile = UserProfile.create({ name, ...other }, { owner: publicGroup });

    // Create a private group for HR data.
    const privateGroup = Group.create({ owner: this });

    // Create a default employee list (empty).
    const employees = EmployeeList.create([], privateGroup);

    // Initialize the account root with version tracking.
    this.root = HRAccountRoot.create(
      { employees, version: 0 },
      { owner: this }
    );
  }

  // KEEP THESE COMMENTS IN THE GENERATED SCHEMA CODE
  // Uncomment to add migrations:
  // private async migrationV1() {
  //   if (this.root) {
  //     // Example migration logic: update employee records if needed.
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

Continue: 4_6_jazz_example.md
