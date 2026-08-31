import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { schema as s, userIdentity } from "jazz-tools";
import type { Db, TransactionScope, WriteResult } from "jazz-tools";
import { createPolicyTestApp } from "jazz-tools/testing";
import type { PolicyTestApp } from "jazz-tools/testing";
import { MemoryRouter } from "react-router";
import type * as ReactRouter from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";

const issuer = "https://inspector-save.test";
const userId = "editor";
const permittedOwner = userIdentity(issuer, userId);
const inspectorSaveApp = s.defineApp({
  todos: s.table({
    title: s.string(),
    owner_id: s.string(),
  }),
});
const inspectorSavePermissions = s.definePermissions(inspectorSaveApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user });
  policy.todos.allowInsert.where({ owner_id: session.user });
  policy.todos.allowUpdate.where({ owner_id: session.user });
});

let currentDb: Db | null = null;

vi.mock("jazz-tools/react", () => ({
  useAll: () => ({ data: [], isLoading: false, error: null }),
  useDb: () => {
    if (!currentDb) throw new Error("Inspector integration Db is not initialized");
    return currentDb;
  },
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => ({
    wasmSchema: inspectorSaveApp.wasmSchema,
    runtime: "standalone",
  }),
}));

vi.mock("react-router", async () => {
  const actual = await vi.importActual<typeof ReactRouter>("react-router");
  return {
    ...actual,
    useParams: () => ({ table: "todos" }),
  };
});

type InstrumentedDb = {
  db: Db;
  transactionCount(): number;
  insertIds: string[];
};

function instrumentDb(db: Db, interruptFirstConfirmation = false): InstrumentedDb {
  let transactionCount = 0;
  let confirmationInterrupted = false;
  const insertIds: string[] = [];

  const transaction = async <TResult,>(
    callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>> => {
    transactionCount += 1;
    const result = await db.transaction((tx) => {
      const instrumentedScope = new Proxy(tx, {
        get(target, property) {
          const value = Reflect.get(target, property, target);
          if (property === "insert" && typeof value === "function") {
            return (...args: unknown[]) => {
              const options = args[2];
              if (options && typeof options === "object" && "id" in options) {
                insertIds.push(String(options.id));
              }
              return Reflect.apply(value, target, args);
            };
          }
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
      return callback(instrumentedScope);
    });

    if (!interruptFirstConfirmation) return result;
    const realWait = result.wait.bind(result);
    // Fault only after the real edge receipt resolves: the runtime has committed,
    // but the Inspector observes the same ambiguity as a lost confirmation.
    return new Proxy(result, {
      get(target, property) {
        if (property === "wait") {
          return async (options: Parameters<typeof realWait>[0]) => {
            const receipt = await realWait(options);
            if (!confirmationInterrupted) {
              confirmationInterrupted = true;
              throw new Error("connection closed after committed receipt");
            }
            return receipt;
          };
        }
        const value = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  };

  const instrumented = new Proxy(db, {
    get(target, property) {
      if (property === "transaction") return transaction;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });

  return {
    db: instrumented,
    transactionCount: () => transactionCount,
    insertIds,
  };
}

function renderGrid() {
  return render(
    <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
      <TableDataGrid />
    </MemoryRouter>,
  );
}

function editStagedTextColumn(columnIndex: number, label: string, value: string): void {
  const stagedRow = screen.getByText("staged").closest('[role="row"], tr');
  expect(stagedRow).not.toBeNull();
  const cells = within(stagedRow as HTMLElement).getAllByRole("gridcell");
  const cell = cells[columnIndex];
  expect(cell).not.toBeUndefined();
  fireEvent.doubleClick(cell as HTMLElement);
  const editor = screen.getByLabelText(`Edit ${label}`);
  fireEvent.change(editor, { target: { value } });
  fireEvent.blur(editor);
}

async function createInspectorDb(): Promise<{ app: PolicyTestApp; db: Db }> {
  const app = await createPolicyTestApp(inspectorSaveApp, inspectorSavePermissions, expect);
  const db = app.as({
    issuer,
    user_id: userId,
    claims: {},
    authMode: "external",
  });
  return { app, db };
}

describe("TableDataGrid real Db save retries", () => {
  let policyApp: PolicyTestApp | null = null;

  afterEach(async () => {
    cleanup();
    currentDb = null;
    await policyApp?.shutdown();
    policyApp = null;
  });

  it("confirms an ambiguously reported insert and updates later staged edits without reinserting", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const instrumented = instrumentDb(setup.db, true);
    currentDb = instrumented.db;
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert row" }));
    editStagedTextColumn(1, "title", "committed once");
    editStagedTextColumn(2, "owner_id", permittedOwner);
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(
      () => {
        expect(screen.getByText("Confirmation pending")).not.toBeNull();
        expect(screen.getByRole("alert").textContent).toContain(
          "connection closed after committed receipt",
        );
      },
      { timeout: 10_000 },
    );
    await expect(
      setup.db.all(inspectorSaveApp.todos.where({ title: "committed once" }), { tier: "edge" }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: instrumented.insertIds[0],
        title: "committed once",
        owner_id: permittedOwner,
      }),
    ]);

    editStagedTextColumn(1, "title", "updated after ambiguity");
    fireEvent.click(screen.getByRole("button", { name: "Retry confirmation" }));
    await waitFor(
      () => {
        expect(screen.queryByText("Confirmation pending")).toBeNull();
        expect(screen.getByRole("button", { name: "Save changes" })).not.toBeNull();
      },
      { timeout: 10_000 },
    );

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    await waitFor(
      () => {
        expect(screen.queryByRole("button", { name: "Save changes" })).toBeNull();
      },
      { timeout: 10_000 },
    );

    await expect(
      setup.db.all(inspectorSaveApp.todos.where({ title: "updated after ambiguity" }), {
        tier: "edge",
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: instrumented.insertIds[0],
        title: "updated after ambiguity",
        owner_id: permittedOwner,
      }),
    ]);
    expect(instrumented.transactionCount()).toBe(2);
    expect(instrumented.insertIds).toHaveLength(1);
  }, 30_000);

  it("reuses a staged id after real authority rejection when the insert becomes permitted", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const instrumented = instrumentDb(setup.db);
    currentDb = instrumented.db;
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert row" }));
    editStagedTextColumn(1, "title", "retry same row");
    editStagedTextColumn(2, "owner_id", "not-the-session-owner");
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(
      () => {
        expect(screen.getByRole("alert").textContent).toMatch(
          /AuthorizationDenied|Write rejected by server authorization/,
        );
        expect(screen.getByRole("button", { name: "Save changes" })).not.toBeNull();
        expect(screen.queryByText("Confirmation pending")).toBeNull();
      },
      { timeout: 10_000 },
    );
    expect(instrumented.transactionCount()).toBe(1);
    expect(instrumented.insertIds).toHaveLength(1);

    editStagedTextColumn(2, "owner_id", permittedOwner);
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));
    await waitFor(
      () => {
        expect(screen.queryByRole("button", { name: "Save changes" })).toBeNull();
      },
      { timeout: 10_000 },
    );

    expect(instrumented.transactionCount()).toBe(2);
    expect(instrumented.insertIds).toEqual([expect.any(String), instrumented.insertIds[0]]);
    await expect(
      setup.db.all(inspectorSaveApp.todos.where({ title: "retry same row" }), { tier: "edge" }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: instrumented.insertIds[0],
        title: "retry same row",
        owner_id: permittedOwner,
      }),
    ]);
  }, 30_000);
});
