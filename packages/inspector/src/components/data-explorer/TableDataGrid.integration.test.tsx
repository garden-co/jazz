import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { schema as s } from "jazz-tools";
import type { Db, TransactionScope, WriteResult } from "jazz-tools";
import { createPolicyTestApp } from "jazz-tools/testing";
import type { PolicyTestApp } from "jazz-tools/testing";
import { MemoryRouter } from "react-router";
import type * as ReactRouter from "react-router";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";
import { GenericQueryBuilder } from "../../utility/generic-query-builder.js";

const issuer = "https://inspector-save.test";
const userId = "editor";
const permittedOwner = "00000000-0000-4000-8000-000000000001";
const inspectorSaveApp = s.defineApp({
  todos: s.table({
    title: s.string(),
    owner_id: s.uuid(),
    rank: s.bigint().optional(),
  }),
});
const inspectorSavePermissions = s.definePermissions(inspectorSaveApp, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner_id: session.user.account });
  policy.todos.allowInsert.where({ owner_id: session.user.account });
  policy.todos.allowUpdate.where({ owner_id: session.user.account });
  policy.todos.allowDelete.where({ owner_id: session.user.account });
});

vi.mock("jazz-tools/react", () => ({
  useAll: () => ({ data: [], isLoading: false, error: null }),
  useDb: () => {
    if (!currentDb) throw new Error("Inspector integration Db is not initialized");
    return currentDb;
  },
}));

let currentDb: Db | null = null;

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
  deleteIds: string[];
};

function instrumentDb(db: Db, interruptFirstConfirmation = false): InstrumentedDb {
  let transactionCount = 0;
  let confirmationInterrupted = false;
  const insertIds: string[] = [];
  const deleteIds: string[] = [];

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
          if (property === "delete" && typeof value === "function") {
            return (...args: unknown[]) => {
              deleteIds.push(String(args[1]));
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
    deleteIds,
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
    account_id: permittedOwner,
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

  it("turns a pre-confirmation staged-row cancellation into a real delete", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const instrumented = instrumentDb(setup.db, true);
    currentDb = instrumented.db;
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert row" }));
    editStagedTextColumn(1, "title", "cancel after ambiguity");
    editStagedTextColumn(2, "owner_id", permittedOwner);
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(
      () => {
        expect(screen.getByText("Confirmation pending")).not.toBeNull();
        expect(screen.getByRole("button", { name: "Retry confirmation" })).not.toBeNull();
      },
      { timeout: 10_000 },
    );
    const insertedId = instrumented.insertIds[0];
    expect(insertedId).toBeDefined();

    fireEvent.click(screen.getByRole("button", { name: "Cancel staged insert" }));
    expect(screen.queryByText("staged")).toBeNull();
    expect(screen.getByText("1 row will be deleted")).not.toBeNull();

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
      setup.db.all(inspectorSaveApp.todos.where({ id: insertedId }), { tier: "edge" }),
    ).resolves.toEqual([]);
    expect(instrumented.transactionCount()).toBe(2);
    expect(instrumented.insertIds).toEqual([insertedId]);
    expect(instrumented.deleteIds).toEqual([insertedId]);
  }, 30_000);

  it("reuses a staged id after real authority rejection when the insert becomes permitted", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const instrumented = instrumentDb(setup.db);
    currentDb = instrumented.db;
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert row" }));
    editStagedTextColumn(1, "title", "retry same row");
    editStagedTextColumn(2, "owner_id", "00000000-0000-4000-8000-000000000002");
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

  it("persists unsafe-range BigInt mutations exactly through the real Db", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const instrumented = instrumentDb(setup.db);
    currentDb = instrumented.db;
    const exactValue = 9007199254740993n;
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert row" }));
    editStagedTextColumn(1, "title", "exact bigint");
    editStagedTextColumn(2, "owner_id", permittedOwner);
    editStagedTextColumn(3, "rank", String(exactValue));
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(
      () => {
        expect(screen.queryByRole("button", { name: "Save changes" })).toBeNull();
      },
      { timeout: 10_000 },
    );

    await expect(setup.db.all(inspectorSaveApp.todos, { tier: "edge" })).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          title: "exact bigint",
          owner_id: permittedOwner,
          rank: exactValue,
        }),
      ]),
    );
  }, 30_000);

  it("applies URL-hydrated BigInt filters through the real query runtime", async () => {
    const setup = await createInspectorDb();
    policyApp = setup.app;
    const exactValue = 9007199254740993n;
    await setup.db
      .insert(inspectorSaveApp.todos, {
        title: "exact rank",
        owner_id: permittedOwner,
        rank: exactValue,
      })
      .wait({ tier: "edge" });
    await setup.db
      .insert(inspectorSaveApp.todos, {
        title: "nearby rank",
        owner_id: permittedOwner,
        rank: exactValue - 1n,
      })
      .wait({ tier: "edge" });

    const filters = JSON.stringify([
      { id: "hydrated-rank", column: "rank", operator: "eq", value: String(exactValue) },
    ]);
    const url = new URL(
      `/data-explorer/todos/data?filters=${encodeURIComponent(filters)}`,
      "https://inspector.test",
    );
    const [hydratedFilter] = JSON.parse(url.searchParams.get("filters") ?? "[]") as Array<{
      column: string;
      value: string;
    }>;
    const query = new GenericQueryBuilder("todos", inspectorSaveApp.wasmSchema).where({
      [hydratedFilter.column]: hydratedFilter.value,
    });
    const rows = await setup.db.all(query, { tier: "edge" });

    expect(rows).toEqual([
      expect.objectContaining({
        title: "exact rank",
        owner_id: permittedOwner,
        rank: exactValue,
      }),
    ]);
  }, 30_000);
});
