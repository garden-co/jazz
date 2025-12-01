import { Group } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
const memberAccount = await createJazzTestAccount();
const Member = co.map({
  name: z.string(),
});

// #region Basic
import { co, z } from "jazz-tools";

const Project = co.map({
  name: z.string(),
  startDate: z.date(),
  status: z.literal(["planning", "active", "completed"]),
  coordinator: co.optional(Member),
});
export type Project = co.loaded<typeof Project>;
export type ProjectInitShape = co.input<typeof Project>; // type accepted by `Project.create`
// #endregion

// #region Records
const Inventory = co.record(z.string(), z.number());
// #endregion

// #region Instantiation
const project = Project.create({
  name: "Spring Planting",
  startDate: new Date("2025-03-15"),
  status: "planning",
});

const inventory = Inventory.create({
  tomatoes: 48,
  basil: 12,
});
// #endregion

// #region Ownership
// Create with default owner (current user)
const privateProject = Project.create({
  name: "My Herb Garden",
  startDate: new Date("2025-04-01"),
  status: "planning",
});

// Create with shared ownership
const gardenGroup = Group.create();
gardenGroup.addMember(memberAccount, "writer");

const communityProject = Project.create(
  {
    name: "Community Vegetable Plot",
    startDate: new Date("2025-03-20"),
    status: "planning",
  },
  { owner: gardenGroup },
);
// #endregion

// #region Reading
console.log(project.name); // "Spring Planting"
console.log(project.status); // "planning"
// #endregion

// #region Optional
if (project.coordinator) {
  console.log(project.coordinator.name); // Safe access
}
// #endregion

// #region Update
project.$jazz.set("name", "Spring Vegetable Garden"); // Update name
project.$jazz.set("startDate", new Date("2025-03-20")); // Update date
// #endregion

// #region ImplicitExplicit
const Dog = co.map({
  name: co.plainText(),
});
const Person = co.map({
  name: co.plainText(),
  dog: Dog,
});

const person = Person.create({
  name: "John",
  dog: { name: "Rex" },
});

// Update the dog field using a CoValue
person.$jazz.set("dog", Dog.create({ name: co.plainText().create("Fido") }));
// Or use a plain JSON object
person.$jazz.set("dog", { name: "Fido" });
// #endregion

// #region TypeSafety
project.$jazz.set("name", "Spring Vegetable Planting"); // ✓ Valid string
// @ts-expect-error
// [!code --]
project.$jazz.set("startDate", "2025-03-15"); // ✗ Type error: expected Date
// [!code --]
// Argument of type 'string' is not assignable to parameter of type 'Date'
// #endregion

// #region DeleteProperties
inventory.$jazz.delete("basil"); // Remove a key-value pair

// For optional fields in struct-like CoMaps
project.$jazz.set("coordinator", undefined); // Remove the reference
// #endregion

// #region Migrations
// @ts-expect-error Duplicate
const Task = co
  .map({
    done: z.boolean(),
    text: co.plainText(),
    version: z.literal([1, 2]),
    priority: z.enum(["low", "medium", "high"]), // new field
  })
  .withMigration((task) => {
    if (task.version === 1) {
      task.$jazz.set("priority", "medium");
      // Upgrade the version so the migration won't run again
      task.$jazz.set("version", 2);
    }
  });
// #endregion

// #region ComplexMigrations
const TaskV1 = co.map({
  version: z.literal(1),
  done: z.boolean(),
  text: z.string(),
});

const TaskV2 = co
  .map({
    // We need to be more strict about the version to make the
    // discriminated union work
    version: z.literal(2),
    done: z.boolean(),
    text: z.string(),
    priority: z.enum(["low", "medium", "high"]),
  })
  .withMigration((task) => {
    // @ts-expect-error - check if we need to run the migration
    if (task.version === 1) {
      task.$jazz.set("version", 2);
      task.$jazz.set("priority", "medium");
    }
  });

// Export the discriminated union; because some users might
// not be able to run the migration
// @ts-expect-error duplicate
export const Task = co.discriminatedUnion("version", [TaskV1, TaskV2]);
export type Task = co.loaded<typeof Task>;
// #endregion
