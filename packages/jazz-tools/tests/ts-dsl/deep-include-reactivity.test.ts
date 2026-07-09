import { afterEach, beforeEach, describe, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createDb, type Db } from "../../src/runtime/db.js";
import { applySubscriptionDelta } from "../../src/runtime/subscription-manager.js";

const schema = {
  orgs: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    org_id: s.ref("orgs"),
  }),
  user_checks: s.table({
    todo_id: s.ref("todos"),
  }),
  check_notes: s.table({
    body: s.string(),
    user_check_id: s.ref("user_checks"),
  }),
};
type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);

type Org = s.RowOf<typeof app.orgs>;
type Todo = s.RowOf<typeof app.todos>;

async function waitForCondition(
  check: () => boolean,
  timeoutMs: number,
  errorMessage: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(errorMessage);
}

function uniqueDbName(label: string): string {
  return `deep-include-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("deep-include reactivity", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "deep-include-reactivity",
      driver: { type: "persistent", dbName: uniqueDbName("repro") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("fires when a depth-1 via dependency is inserted (baseline)", async () => {
    const {
      value: { id: orgId },
    } = db.insert(app.orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(app.todos, { title: "ship it", org_id: orgId });

    const current: Todo[] = [];
    const snapshots: Todo[][] = [];
    const unsubscribe = db.subscribeAll(app.todos.include({ user_checksViaTodo: true }), (delta) =>
      snapshots.push([...applySubscriptionDelta(current, delta)]),
    );

    await waitForCondition(
      () => snapshots.length > 0 && snapshots[snapshots.length - 1]!.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = snapshots.length;
    const {
      value: { id: userCheckId },
    } = db.insert(app.user_checks, { todo_id: todoId });

    await waitForCondition(
      () => {
        if (snapshots.length <= initialSnapshotCount) return false;
        const latest = snapshots[snapshots.length - 1]!;
        const todo = latest[0] as
          | undefined
          | {
              user_checksViaTodo?: Array<{ id: string }>;
            };
        const userChecks = todo?.user_checksViaTodo;
        return Array.isArray(userChecks) && userChecks.some((check) => check.id === userCheckId);
      },
      4000,
      "expected depth-1 subscription to deliver fresh nested user_checks",
    );

    unsubscribe();
  });

  it("fires when a depth-2 via dependency is inserted", async () => {
    const {
      value: { id: orgId },
    } = db.insert(app.orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(app.todos, { title: "ship it", org_id: orgId });

    const current: Org[] = [];
    const snapshots: Org[][] = [];
    const unsubscribe = db.subscribeAll(
      app.orgs.include({ todosViaOrg: { user_checksViaTodo: true } }),
      (delta) => snapshots.push([...applySubscriptionDelta(current, delta)]),
    );

    await waitForCondition(
      () => snapshots.length > 0 && snapshots[snapshots.length - 1]!.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = snapshots.length;
    const {
      value: { id: userCheckId },
    } = db.insert(app.user_checks, { todo_id: todoId });

    await waitForCondition(
      () => {
        if (snapshots.length <= initialSnapshotCount) return false;
        const latest = snapshots[snapshots.length - 1]!;
        const org = latest[0] as
          | undefined
          | {
              todosViaOrg?: Array<{
                user_checksViaTodo?: Array<{ id: string }>;
              }>;
            };
        const userChecks = org?.todosViaOrg?.[0]?.user_checksViaTodo;
        return Array.isArray(userChecks) && userChecks.some((check) => check.id === userCheckId);
      },
      4000,
      "expected depth-2 subscription to deliver fresh nested user_checks",
    );

    unsubscribe();
  });

  it("fires when a depth-3 via dependency is inserted", async () => {
    const {
      value: { id: orgId },
    } = db.insert(app.orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(app.todos, { title: "ship it", org_id: orgId });
    const {
      value: { id: userCheckId },
    } = db.insert(app.user_checks, { todo_id: todoId });

    const current: Org[] = [];
    const snapshots: Org[][] = [];
    const unsubscribe = db.subscribeAll(
      app.orgs.include({
        todosViaOrg: { user_checksViaTodo: { check_notesViaUser_check: true } },
      }),
      (delta) => snapshots.push([...applySubscriptionDelta(current, delta)]),
    );

    await waitForCondition(
      () => snapshots.length > 0 && snapshots[snapshots.length - 1]!.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = snapshots.length;
    const {
      value: { id: noteId },
    } = db.insert(app.check_notes, { body: "looks good", user_check_id: userCheckId });

    await waitForCondition(
      () => {
        if (snapshots.length <= initialSnapshotCount) return false;
        const latest = snapshots[snapshots.length - 1]!;
        const org = latest[0] as
          | undefined
          | {
              todosViaOrg?: Array<{
                user_checksViaTodo?: Array<{
                  check_notesViaUser_check?: Array<{ id: string }>;
                }>;
              }>;
            };
        const notes = org?.todosViaOrg?.[0]?.user_checksViaTodo?.[0]?.check_notesViaUser_check;
        return Array.isArray(notes) && notes.some((n) => n.id === noteId);
      },
      4000,
      "expected depth-3 subscription to deliver fresh nested check_notes",
    );

    unsubscribe();
  });
});
