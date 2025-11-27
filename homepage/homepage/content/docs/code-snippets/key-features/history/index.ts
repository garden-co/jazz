import { Task } from "./schema";
const task = Task.create({ title: "New task", status: "todo" });

// #region GetEdits
task.$jazz.getEdits().status;
// Returns the latest edit

task.$jazz.getEdits().status?.all;
// Returns array of all edits in chronological order

// Check if edits exist
const statusEdits = task.$jazz.getEdits().status;
if (statusEdits && statusEdits.by?.profile.$isLoaded) {
  const name = statusEdits.by.profile.name;
  console.log(`Last changed by ${name}`);
}
// #endregion

// #region EditDetails
const edit = task.$jazz.getEdits().status;

// The edit object contains:
edit?.value; // The new value: "in-progress"
edit?.by; // Account that made the change
edit?.madeAt; // Date when the change occurred
// #endregion

// #region Latest
// Direct access to latest edit
const latest = task.$jazz.getEdits().title;
if (latest) {
  console.log(`Title is now "${latest.value}"`);
}
// #endregion

// #region All
// Get all edits (chronologically)
const allStatusEdits = task.$jazz.getEdits().status?.all || [];

allStatusEdits.forEach((edit, index) => {
  console.log(`Edit ${index}: ${edit.value} at ${edit.madeAt.toISOString()}`);
});
// Edit 0: todo at 2025-05-22T13:00:00.000Z
// Edit 1: in-progress at 2025-05-22T14:00:00.000Z
// Edit 2: completed at 2025-05-22T15:30:00.000Z
// #endregion

// #region Initial
const allEdits = task.$jazz.getEdits().status?.all || [];
const initialValue = allEdits[0]?.value;
console.log(`Started as: ${initialValue}`);
// Started as: todo
// #endregion

// #region CreatedAtLastUpdatedAt
console.log(new Date(task.$jazz.createdAt));
console.log(new Date(task.$jazz.lastUpdatedAt));
// #endregion
