import type { Db } from "jazz-tools";
import { keyless, type Jazz } from "jazz-tools/backend";
import { unreadableRows } from "jazz-tools/client";
import { app } from "../jazz.generated.js";

// #region e2ee-read-write-ts
export async function createProjectWithTask(db: Db) {
  // Jazz adds you as the project's first member, and your first encrypted write makes its key.
  const project = await db.insert(app.projects, { name: "Spring tour" }).wait({ tier: "global" });
  const projectId = project.id;

  await db.insert(app.tasks, { projectId, title: "Call the venue" }).wait({ tier: "global" });
  // tasks[0].title is "Call the venue". The server stored only the encrypted value.
  return db.all(app.tasks.where({ projectId, title: "Call the venue" }));
}
// #endregion e2ee-read-write-ts

// #region e2ee-unreadable-rows-ts
export async function readTaskPage(db: Db, projectId: string) {
  const page = await db.all(app.tasks.where({ projectId }).limit(20));
  const missing = unreadableRows(page);
  // When `missing` isn't empty, this page is incomplete: `page.length + missing.length` rows matched.
  return { page, missing };
}
// #endregion e2ee-unreadable-rows-ts

// #region e2ee-keyless-backend-ts
const backendApp = keyless(app);

export async function readTasksOnBackend(jazz: Jazz) {
  return jazz.db.all(backendApp.tasks); // each task.title is undefined
}
// #endregion e2ee-keyless-backend-ts

// #region e2ee-add-members-ts
export async function shareProject(db: Db, otherAccountId: string) {
  const project = await db.insert(app.projects, { name: "Plans" }).wait({ tier: "global" });
  await db.encryption.members
    .add(app.projects, project.id, otherAccountId)
    .wait({ tier: "global" });
  return db.encryption.members.list(app.projects, project.id);
}
// #endregion e2ee-add-members-ts

// #region e2ee-upsert-creator-ts
export async function upsertProject(db: Db, projectId: string, accountId: string) {
  return db.transaction((tx) => {
    tx.upsert(app.projects, projectId, { name: "Plans" });
    return tx.encryption.members.add(app.projects, projectId, accountId);
  });
}
// #endregion e2ee-upsert-creator-ts

// #region e2ee-add-member-in-transaction-ts
export async function addMemberWithOtherWrites(db: Db, projectId: string, otherAccountId: string) {
  const written = await db.transaction(async (tx) => {
    await tx.encryption.members.add(app.projects, projectId, otherAccountId);
  });
  await written.wait({ tier: "global" });
}
// #endregion e2ee-add-member-in-transaction-ts

// #region e2ee-deliver-keys-ts
export async function deliverKeysNow(db: Db) {
  const { delivered } = await db.encryption.keys.deliver();
  return delivered;
}
// #endregion e2ee-deliver-keys-ts

// #region e2ee-remove-member-ts
export async function removeMember(db: Db, projectId: string, otherAccountId: string) {
  const removed = await db.encryption.members.remove(app.projects, projectId, otherAccountId);
  if (removed > 0) {
    try {
      await db.encryption.keys.rotate({ table: "projects", id: projectId });
    } catch (error) {
      // `rotation-needs-key`: try again once this device holds the key.
      // Until the key changes, new writes still use the old one.
      console.warn("key not rotated", (error as { code?: string }).code);
    }
  }
  return removed;
}
// #endregion e2ee-remove-member-ts

// #region e2ee-on-report-ts
export function watchEncryptionFailures(db: Db) {
  return db.encryption.onReport((report) => {
    if (report.type === "rotation-check-failed")
      console.warn("rotation check failed", report.space, report.reason);
    if (report.type === "rotation-failed")
      console.warn("key not rotated", report.space, report.reason);
    if (report.type === "delivery-failed")
      console.warn("key delivery failed", report.space, report.reason);
  });
}
// #endregion e2ee-on-report-ts
