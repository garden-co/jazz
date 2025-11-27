import { Account, CoMapEdit } from "jazz-tools";
import { Project, Task } from "./schema";
const task = Task.create({ title: "New task", status: "todo" });

// TODO: These types look ugly, and like there should be a better way.
// #region AuditLogs
function getAuditLog(task: Task) {
  const changes: {
    field: string;
    value: Task[keyof Task] | undefined;
    by: Account | null;
    at: Date;
  }[] = [];

  // Collect edits for all fields
  const fields = Object.keys(task);
  const edits = task.$jazz.getEdits();
  for (const field of fields) {
    const editField = field as keyof typeof edits;
    if (!edits[editField]) continue;

    for (const edit of edits[editField].all) {
      changes.push({
        field,
        value: edit.value,
        by: edit.by,
        at: edit.madeAt,
      });
    }
  }

  // Sort by timestamp (newest first)
  return changes.sort((a, b) => b.at.getTime() - a.at.getTime());
}

// Use it to show change history
const auditLog = getAuditLog(task);
auditLog.forEach((entry) => {
  if (!entry.by?.profile?.$isLoaded) return;
  const when = entry.at.toLocaleString();
  const who = entry.by.profile.name;
  const what = entry.field;
  const value = entry.value;

  console.log(`${when} - ${who} changed ${what} to "${value}"`);
  // 22/05/2025, 12:00:00 - Alice changed title to "New task"
});
// #endregion

const project = Project.create({ name: "New project", status: "todo" });
const myProjects = [project];

// #region GetRecentActivity
function getRecentActivity(projects: Project[], since: Date) {
  const activity: {
    project: string;
    field: string;
    value: Task[keyof Task] | undefined;
    by: Account | null;
    at: Date;
  }[] = [];

  for (const project of projects) {
    // Get all fields that might have edits
    const fields = Object.keys(project);

    // Check each field for edit history
    const edits = project.$jazz.getEdits();
    for (const field of fields) {
      const editField = field as keyof typeof edits;
      // Skip if no edits exist for this field
      if (!edits[editField]) continue;

      for (const edit of edits[editField].all) {
        // Only include edits made after the 'since' date
        if (edit.madeAt > since) {
          activity.push({
            project: project.name,
            field,
            value: edit.value,
            by: edit.by,
            at: edit.madeAt,
          });
        }
      }
    }
  }

  return activity.sort((a, b) => b.at.getTime() - a.at.getTime());
}

// Show activity from the last hour
const hourAgo = new Date(Date.now() - 60 * 60 * 1000);
const recentActivity = getRecentActivity(myProjects, hourAgo);
// [{
//   project: "New project",
//   field: "name",
//   value: "New project",
//   by: Account,
//   at: Date
// }]
// # endregion

// #region GetLastUpdated
function getLastUpdated(task: Task) {
  // Find the most recent edit across all fields
  let lastEdit: CoMapEdit<unknown> | null = null;

  const edits = task.$jazz.getEdits();
  for (const field of Object.keys(task)) {
    const editField = field as keyof typeof edits;
    // Skip if no edits exist for this field
    if (!edits[editField]) continue;

    const fieldEdit = edits[editField];
    if (fieldEdit && (!lastEdit || fieldEdit.madeAt > lastEdit.madeAt)) {
      lastEdit = fieldEdit;
    }
  }

  if (!lastEdit || !lastEdit.by?.profile?.$isLoaded) return null;

  return {
    updatedBy: lastEdit.by.profile.name,
    updatedAt: lastEdit.madeAt,
    message: `Last updated by ${lastEdit.by.profile.name} at ${lastEdit.madeAt.toLocaleString()}`,
  };
}

const lastUpdated = getLastUpdated(task);
console.log(lastUpdated?.message);
// "Last updated by Alice at 22/05/2025, 12:00:00"
// #endregion

// #region QueryHistory
// Find when a task was completed
function findCompletionTime(task: Task): Date | null {
  const statusEdits = task.$jazz.getEdits().status;
  if (!statusEdits) return null;

  // find() returns the FIRST completion time
  // If status toggles (completed → in-progress → completed),
  // this gives you the earliest completion, not the latest
  const completionEdit = statusEdits.all.find(
    (edit) => edit.value === "completed",
  );

  return completionEdit?.madeAt || null;
}

// To get the LATEST completion time instead reverse the array, then find:
function findLatestCompletionTime(task: Task): Date | null {
  const statusEdits = task.$jazz.getEdits().status;
  if (!statusEdits) return null;

  // Reverse and find (stops at first match)
  const latestCompletionEdit = statusEdits.all
    .slice() // Create copy to avoid mutating original
    .reverse()
    .find((edit) => edit.value === "completed");

  return latestCompletionEdit?.madeAt || null;
}

console.log(findCompletionTime(task)); // First completion
console.log(findLatestCompletionTime(task)); // Most recent completion

// Find who made a specific change
function findWhoChanged(task: Task, field: string, value: any) {
  const taskEdits = task.$jazz.getEdits();
  const fieldEdits = taskEdits[field as keyof typeof taskEdits];
  if (!fieldEdits) return null;

  const matchingEdit = fieldEdits.all.find((edit) => edit.value === value);
  return matchingEdit?.by || null;
}
const account = findWhoChanged(task, "status", "completed");
if (account?.profile?.$isLoaded) {
  console.log(account.profile.name);
}
// Alice
// #endregion
