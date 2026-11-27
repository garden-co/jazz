import { acceptDeviceAccounts, type CreateJazzOptions } from "../../src/backend/index.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import type { LocalFirstLoader } from "../../src/runtime/local-first-upgrade.js";
import { serveEncryptedTestApp, testApp } from "../../src/testing/index.js";

export const thinHandleApp = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes", __e2ee_eq_title: "bytes", note: "string?" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: {
        space: "projectId",
        scope: "projects",
        columns: { title: "string" },
        equality: ["title"],
      },
    },
    // Plain; a comment's insert is allowed only once its task is on the server.
    comments: {
      columns: { taskId: "uuid", text: "string" },
      relations: { task: { rel: "tasks", column: "taskId" } },
    },
    notes: { columns: { text: "string" } },
    // Plain; a reply's insert is allowed only once its note is on the server.
    replies: {
      columns: { noteId: "uuid", text: "string" },
      relations: { note: { rel: "notes", column: "noteId" } },
    },
  },
});
const members =
  "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)";
export const thinHandlePermissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: members,
      insert: "$createdBy.account = session.user",
      update: "ownerId = session.user",
    },
    // Anyone reads a task's row, so an account outside its space sees its
    // plain columns and holds no key for its encrypted one.
    tasks: { read: "true", insert: "allowedTo.read(project)" },
    comments: { read: "true", insert: "allowedTo.read(task)" },
    notes: { read: "true", insert: "true" },
    replies: { read: "true", insert: "allowedTo.read(note)" },
  },
};

export const serveThinTestApp = (
  app: CreateJazzOptions["app"],
  permissions: CreateJazzOptions["permissions"],
  cleanups: Array<() => Promise<unknown>>,
) =>
  serveEncryptedTestApp(
    {
      app,
      permissions,
      authenticate(request) {
        const match = /^Bearer token-(\w+)$/.exec(request.headers.get("authorization") ?? "");
        return match
          ? { userId: match[1]!, expiresAt: Math.floor(Date.now() / 1000) + 3600 }
          : acceptDeviceAccounts(request);
      },
    },
    cleanups,
  );

/** A loader that loads only once `release()` is called, counting its calls. */
export function gatedLoader() {
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  const gate = {
    calls: 0,
    release: () => release(),
    loader: async () => {
      gate.calls++;
      await released;
      return { createClient: createLocalFirstDb };
    },
  } satisfies { calls: number; release: () => void; loader: LocalFirstLoader };
  return gate;
}
