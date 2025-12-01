import { Group } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
const colleagueAccount = await createJazzTestAccount();
const Task = co.map({
  title: z.string(),
  status: z.literal(["todo", "in-progress", "complete"]),
});

// #region Basic
import { co, z } from "jazz-tools";

const ListOfResources = co.list(z.string());
export type ListOfResources = co.loaded<typeof ListOfResources>;

const ListOfTasks = co.list(Task);
export type ListOfTasks = co.loaded<typeof ListOfTasks>;
export type ListOfTasksInitShape = co.input<typeof ListOfTasks>; // type accepted by `ListOfTasks.create`
// #endregion

// #region Create
// Create an empty list
const resources = co.list(z.string()).create([]);

// Create a list with initial items
const tasks = co.list(Task).create([
  { title: "Prepare soil beds", status: "in-progress" },
  { title: "Order compost", status: "todo" },
]);
// #endregion

// #region WithOwner
// Create with shared ownership
const teamGroup = Group.create();
teamGroup.addMember(colleagueAccount, "writer");

const teamList = co.list(Task).create([], { owner: teamGroup });
// #endregion

// #region ArrayAccess
// Access by index
const firstTask = tasks[0];
console.log(firstTask.title); // "Prepare soil beds"

// Get list length
console.log(tasks.length); // 2

// Iteration
tasks.forEach((task) => {
  console.log(task.title);
  // "Prepare soil beds"
  // "Order compost"
});

// Array methods
const todoTasks = tasks.filter((task) => task.status === "todo");
console.log(todoTasks.length); // 1
// #endregion

// #region Updating
// Add items
resources.$jazz.push("Tomatoes"); // Add to end
resources.$jazz.unshift("Lettuce"); // Add to beginning
tasks.$jazz.push({
  // Add complex items
  title: "Install irrigation", // (Jazz will create
  status: "todo", // the CoValue for you!)
});

// Replace items
resources.$jazz.set(0, "Cucumber"); // Replace by index

// Modify nested items
tasks[0].$jazz.set("status", "complete"); // Update properties of references
// #endregion

// #region Deletion
// Remove items
resources.$jazz.remove(2); // By index
console.log(resources); // ["Cucumber", "Peppers"]
resources.$jazz.remove((item) => item === "Cucumber"); // Or by predicate
console.log(resources); // ["Tomatoes", "Peppers"]

// Keep only items matching the predicate
resources.$jazz.retain((item) => item !== "Cucumber");
console.log(resources); // ["Tomatoes", "Peppers"]
// #endregion

// #region SpliceAndShift
// Remove 2 items starting at index 1
resources.$jazz.splice(1, 2);
console.log(resources); // ["Tomatoes"]

// Remove a single item at index 0
resources.$jazz.splice(0, 1);
console.log(resources); // ["Cucumber", "Peppers"]

// Remove items
const lastItem = resources.$jazz.pop(); // Remove and return last item
resources.$jazz.shift(); // Remove first item
// #endregion

// #region PushFindFilter
// Add multiple items at once
resources.$jazz.push("Tomatoes", "Basil", "Peppers");

// Find items
const basil = resources.find((r) => r === "Basil");

// Filter (returns regular array, not a CoList)
const tItems = resources.filter((r) => r.startsWith("T"));
console.log(tItems); // ["Tomatoes"]
// #endregion

// #region TypeSafety
// TypeScript catches type errors
resources.$jazz.push("Carrots"); // ✓ Valid string
// @ts-expect-error
// [!code --]
resources.$jazz.push(42); // ✗ Type error: expected string
// [!code --]
// Argument of type 'number' is not assignable to parameter of type 'string'
// For lists of references
tasks.forEach((task) => {
  console.log(task.title); // TypeScript knows task has title
});
// #endregion
