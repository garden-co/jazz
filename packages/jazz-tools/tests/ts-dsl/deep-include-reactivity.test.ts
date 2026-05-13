import { afterEach, beforeEach, describe, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { SubscriptionDelta } from "../../src/runtime/subscription-manager.js";
import type { WasmSchema } from "../../src/drivers/types.js";

const schema: WasmSchema = {
  orgs: {
    columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
  },
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "org_id", column_type: { type: "Uuid" }, nullable: false, references: "orgs" },
    ],
  },
  user_checks: {
    columns: [
      { name: "todo_id", column_type: { type: "Uuid" }, nullable: false, references: "todos" },
    ],
  },
  check_notes: {
    columns: [
      { name: "body", column_type: { type: "Text" }, nullable: false },
      {
        name: "user_check_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "user_checks",
      },
    ],
  },
};

interface Org {
  id: string;
  name: string;
}
interface Todo {
  id: string;
  title: string;
  org_id: string;
}
interface UserCheck {
  id: string;
  todo_id: string;
}
interface CheckNote {
  id: string;
  body: string;
  user_check_id: string;
}

const orgs: TableProxy<Org, Omit<Org, "id">> = {
  _table: "orgs",
  _schema: schema,
  _rowType: {} as Org,
  _initType: {} as Omit<Org, "id">,
};
const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};
const userChecks: TableProxy<UserCheck, Omit<UserCheck, "id">> = {
  _table: "user_checks",
  _schema: schema,
  _rowType: {} as UserCheck,
  _initType: {} as Omit<UserCheck, "id">,
};
const checkNotes: TableProxy<CheckNote, Omit<CheckNote, "id">> = {
  _table: "check_notes",
  _schema: schema,
  _rowType: {} as CheckNote,
  _initType: {} as Omit<CheckNote, "id">,
};

function makeQuery<T>(
  table: string,
  body: { includes?: Record<string, boolean | object> },
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as T,
    _build() {
      return JSON.stringify({
        table,
        conditions: [],
        includes: body.includes ?? {},
        orderBy: [],
      });
    },
  };
}

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
    } = db.insert(orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(todos, { title: "ship it", org_id: orgId });

    const deltas: Array<SubscriptionDelta<Todo>> = [];
    const unsubscribe = db.subscribeAll(
      makeQuery<Todo>("todos", { includes: { user_checksViaTodo: true } }),
      (delta) => deltas.push(delta),
    );

    await waitForCondition(
      () => deltas.length > 0 && deltas[deltas.length - 1]!.all.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = deltas.length;
    db.insert(userChecks, { todo_id: todoId });

    await waitForCondition(
      () => deltas.length > initialSnapshotCount,
      4000,
      "expected depth-1 subscription to fire on user_checks insert",
    );

    unsubscribe();
  });

  it("fires when a depth-2 via dependency is inserted", async () => {
    const {
      value: { id: orgId },
    } = db.insert(orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(todos, { title: "ship it", org_id: orgId });

    const deltas: Array<SubscriptionDelta<Org>> = [];
    const unsubscribe = db.subscribeAll(
      makeQuery<Org>("orgs", {
        includes: { todosViaOrg: { user_checksViaTodo: true } },
      }),
      (delta) => deltas.push(delta),
    );

    await waitForCondition(
      () => deltas.length > 0 && deltas[deltas.length - 1]!.all.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = deltas.length;
    db.insert(userChecks, { todo_id: todoId });

    await waitForCondition(
      () => deltas.length > initialSnapshotCount,
      4000,
      "expected depth-2 subscription to fire on user_checks insert",
    );

    unsubscribe();
  });

  it("fires when a depth-3 via dependency is inserted", async () => {
    const {
      value: { id: orgId },
    } = db.insert(orgs, { name: "Acme" });
    const {
      value: { id: todoId },
    } = db.insert(todos, { title: "ship it", org_id: orgId });
    const {
      value: { id: userCheckId },
    } = db.insert(userChecks, { todo_id: todoId });

    const deltas: Array<SubscriptionDelta<Org>> = [];
    const unsubscribe = db.subscribeAll(
      makeQuery<Org>("orgs", {
        includes: {
          todosViaOrg: { user_checksViaTodo: { check_notesViaUser_check: true } },
        },
      }),
      (delta) => deltas.push(delta),
    );

    await waitForCondition(
      () => deltas.length > 0 && deltas[deltas.length - 1]!.all.length === 1,
      4000,
      "expected initial snapshot",
    );

    const initialSnapshotCount = deltas.length;
    const {
      value: { id: noteId },
    } = db.insert(checkNotes, { body: "looks good", user_check_id: userCheckId });

    await waitForCondition(
      () => {
        if (deltas.length <= initialSnapshotCount) return false;
        const latest = deltas[deltas.length - 1]!;
        const org = latest.all[0] as
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
