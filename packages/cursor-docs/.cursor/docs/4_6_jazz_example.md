# Example app 6: A task management app that helps users organize their to-dos with categories, tags, due dates, and priority levels

```typescript
import { Account, CoList, CoMap, Group, Profile, coField } from "jazz-tools";

// Task priority levels
export type PriorityLevel = "Low" | "Medium" | "High";

// Represents a tag that can be associated with tasks
export class Tag extends CoMap {
  name = coField.string;
  deleted = coField.boolean;
}

export class TagList extends CoList.Of(coField.ref(Tag)) {}

// Represents a category that can group tasks
export class Category extends CoMap {
  name = coField.string;
  deleted = coField.boolean;
}

export class CategoryList extends CoList.Of(coField.ref(Category)) {}

// Represents a single task in the todo app
export class Task extends CoMap {
  title = coField.string;
  description = coField.optional.string;
  dueDate = coField.optional.Date;
  isCompleted = coField.boolean;
  priority = coField.literal("Low", "Medium", "High");
  tags = coField.ref(TagList);
  category = coField.optional.ref(Category);
  deleted = coField.boolean;
}

export class TaskList extends CoList.Of(coField.ref(Task)) {}

// Container for organizing tasks, categories and tags
export class Container extends CoMap {
  tasks = coField.ref(TaskList);
  categories = coField.ref(CategoryList);
  tags = coField.ref(TagList);
}

// Root structure holding all data
export class AccountRoot extends CoMap {
  container = coField.ref(Container);
  version = coField.optional.number;
}

export class UserProfile extends Profile {
  name = coField.string;

  static validate(data: { name?: string; other?: Record<string, unknown> }) {
    const errors: string[] = [];
    if (!data.name?.trim()) {
      errors.push("Please enter a name.");
    }
    return { errors };
  }
}

// Main account class that handles data initialization
export class JazzAccount extends Account {
  profile = coField.ref(UserProfile);
  root = coField.ref(AccountRoot);

  async migrate(creationProps?: {
    name: string;
    other?: Record<string, unknown>;
  }) {
    if (!this._refs.root && creationProps) {
      await this.initialMigration(creationProps);
      return;
    }

    // uncomment this to add migrations
    // const currentVersion = this.root?.version || 0;
    // if (currentVersion < 1) {
    //   await this.migrationV1();
    // }
    // if (currentVersion < 2) {
    //   await this.migrationV2();
    // }
  }

  private async initialMigration(
    creationProps: {
      name: string;
      other?: Record<string, unknown>;
    }
  ) {
    const { name, other } = creationProps;
    const profileErrors = UserProfile.validate({ name, ...other });
    if (profileErrors.errors.length > 0) {
      throw new Error(
        "Invalid profile data: " + profileErrors.errors.join(", "),
      );
    }

    const publicGroup = Group.create({ owner: this });
    publicGroup.addMember("everyone", "reader");

    this.profile = UserProfile.create(
      {
        name,
        ...other,
      },
      { owner: publicGroup },
    );

    const privateGroup = Group.create({ owner: this });

    // Create default container with empty lists
    const defaultContainer = Container.create(
      {
        tasks: TaskList.create([], privateGroup),
        categories: CategoryList.create([], privateGroup),
        tags: TagList.create([], privateGroup),
      },
      privateGroup,
    );

    // Initialize root structure with version
    this.root = AccountRoot.create({
      container: defaultContainer,
      version: 0, // Set initial version
      // here owner is always "this" Account
    }, { owner: this });
  }

  // uncomment this to add migrations
  // private async migrationV1() {
  //   if (this.root) {
  //     // Add migration logic here
  //     this.root.version = 1;
  //   }
  // }

  // private async migrationV2() {
  //   if (this.root) {
  //     // Future migration logic here
  //     this.root.version = 2;
  //   }
  // }
}
```
