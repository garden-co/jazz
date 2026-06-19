import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";

const mockUseAll = vi.fn();
const mockUpdate = vi.fn();
const mockInsert = vi.fn();
const mockDelete = vi.fn();
const mockUpdateWait = vi.fn();
const mockInsertWait = vi.fn();
const mockDeleteWait = vi.fn();
let currentRows: Array<Record<string, unknown>>;
let currentReferenceRowsByTable: Record<string, Array<Record<string, unknown>>>;

function getContainingCell(element: HTMLElement | null): HTMLElement | null {
  return element?.closest('[role="gridcell"], td') ?? null;
}

function getContainingRow(element: HTMLElement | null): HTMLElement | null {
  return element?.closest('[role="row"], tr') ?? null;
}

function getCellsInRowContaining(text: string): HTMLElement[] {
  const row = getContainingRow(screen.getByText(text));
  expect(row).not.toBeNull();
  return within(row as HTMLElement).getAllByRole("gridcell");
}

function getLastTodosQuery(): { _build: () => string } {
  return [...mockUseAll.mock.calls]
    .reverse()
    .map((call) => call[0] as { _build: () => string })
    .find((query) => {
      if (!query || typeof query !== "object" || !("_build" in query)) return false;
      return JSON.parse(query._build()).table === "todos";
    })!;
}

function renderGridUi() {
  return (
    <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
      <TableDataGrid />
    </MemoryRouter>
  );
}

function renderGrid() {
  return render(renderGridUi());
}

const mockWasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "meta", column_type: { type: "Row", columns: [] }, nullable: true },
      { name: "owner_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
      { name: "blob", column_type: { type: "Bytea" }, nullable: true },
      {
        name: "status",
        column_type: { type: "Enum", variants: ["open", "closed"] },
        nullable: true,
      },
    ],
  },
  users: {
    columns: [
      { name: "displayName", column_type: { type: "Text" }, nullable: false },
      { name: "email", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

vi.mock("jazz-tools/react", () => ({
  useAll: (...args: unknown[]) => mockUseAll(...args),
  useDb: () => ({
    update: (...args: unknown[]) => mockUpdate(...args),
    insert: (...args: unknown[]) => mockInsert(...args),
    delete: (...args: unknown[]) => mockDelete(...args),
  }),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => ({
    wasmSchema: mockWasmSchema,
    runtime: "extension",
    queryPropagation: "local-only",
  }),
}));

vi.mock("react-router", async () => {
  const actual = await vi.importActual<typeof import("react-router")>("react-router");
  return {
    ...actual,
    useParams: () => ({ table: "todos" }),
  };
});

describe("TableDataGrid", () => {
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  beforeEach(() => {
    currentRows = [
      {
        id: "row-2",
        title: "zeta",
        done: false,
        meta: { done: true },
        owner_id: "owner-a",
        blob: new Uint8Array([1, 2]),
        status: "open",
      },
      {
        id: "row-1",
        title: "alpha",
        done: true,
        meta: null,
        owner_id: "owner-b",
        blob: new Uint8Array([5, 6]),
        status: "closed",
      },
    ];
    currentReferenceRowsByTable = {
      users: [
        { id: "owner-a", displayName: "Alice", email: "alice@example.test" },
        { id: "owner-b", displayName: "Bob", email: "bob@example.test" },
      ],
    };

    mockUpdate.mockReset();
    mockInsert.mockReset();
    mockDelete.mockReset();
    mockUpdateWait.mockReset();
    mockInsertWait.mockReset();
    mockDeleteWait.mockReset();
    mockUpdateWait.mockResolvedValue(undefined);
    mockInsertWait.mockResolvedValue(undefined);
    mockDeleteWait.mockResolvedValue(undefined);
    mockUpdate.mockImplementation(() => ({
      wait: (...args: unknown[]) => mockUpdateWait(...args),
    }));
    mockInsert.mockImplementation(() => ({
      wait: (...args: unknown[]) => mockInsertWait(...args),
    }));
    mockDelete.mockImplementation(() => ({
      wait: (...args: unknown[]) => mockDeleteWait(...args),
    }));
    mockUseAll.mockReset();
    mockUseAll.mockImplementation((query) => {
      const builtQuery =
        query &&
        typeof query === "object" &&
        "_build" in query &&
        typeof query._build === "function"
          ? JSON.parse(query._build())
          : null;

      if (!builtQuery || builtQuery.table === "todos") {
        return currentRows;
      }

      const tableRows = currentReferenceRowsByTable[builtQuery.table] ?? [];
      return tableRows.filter((row) =>
        builtQuery.conditions.every((condition: { column: string; op: string; value: unknown }) => {
          if (condition.op !== "eq") {
            return true;
          }
          return row[condition.column] === condition.value;
        }),
      );
    });
  });

  it("renders schema-derived columns and reactive rows", () => {
    renderGrid();

    expect(screen.queryByText("6 columns · 2 rows on page · 0 filters")).toBeNull();
    expect(screen.getByRole("region", { name: "Filter rows" })).not.toBeNull();
    expect(screen.getByRole("link", { name: "Schema" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "Insert" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: /ID/ })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "title" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "done" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "meta" })).not.toBeNull();
    expect(screen.getByText("row-2")).not.toBeNull();
    expect(screen.getByText("zeta")).not.toBeNull();
    expect(screen.getByText('{"done":true}')).not.toBeNull();
    expect((screen.getByLabelText("Rows per page") as HTMLSelectElement).value).toBe("25");
  });

  it("renders null cell values with a marker", () => {
    renderGrid();

    const row = getContainingRow(screen.getByText("row-1"));
    expect(row).not.toBeNull();

    const nullMarker = within(row as HTMLElement).getByText("<null>");
    expect(getContainingCell(nullMarker)).not.toBeNull();
  });

  it("renders reference cells as links to the related table filtered by id", () => {
    renderGrid();

    expect(screen.getByText("Alice")).not.toBeNull();
    const relationLink = screen.getByRole("link", { name: "Open Alice in users" });
    const href = relationLink.getAttribute("href");
    expect(href).not.toBeNull();

    const url = new URL(href!, "https://inspector.test");
    expect(url.pathname).toBe("/data-explorer/users/data");
    expect(JSON.parse(url.searchParams.get("filters") ?? "[]")).toMatchObject([
      {
        column: "id",
        operator: "eq",
        value: "owner-a",
      },
    ]);
  });

  it("updates query sorting when a sortable column header is clicked", () => {
    renderGrid();

    const firstQuery = mockUseAll.mock.calls[0]?.[0] as { _build: () => string };
    expect(JSON.parse(firstQuery._build())).toMatchObject({
      orderBy: [["id", "asc"]],
      limit: 26,
      offset: 0,
    });

    const titleHeader = screen.getByRole("columnheader", { name: "title" });
    fireEvent.click(titleHeader);

    const sortedQuery = getLastTodosQuery();
    expect(JSON.parse(sortedQuery._build())).toMatchObject({
      orderBy: [["title", "asc"]],
      limit: 26,
      offset: 0,
    });
  });

  it("subscribes with local-only propagation in extension mode", () => {
    renderGrid();

    expect(mockUseAll).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        propagation: "local-only",
        visibility: "hidden_from_live_query_list",
      }),
    );
  });

  it("adds a where clause and compiles it into query conditions", () => {
    renderGrid();

    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "title" } });
    fireEvent.change(screen.getByLabelText("Operator"), { target: { value: "contains" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "alpha" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    const filteredQuery = getLastTodosQuery();
    expect(JSON.parse(filteredQuery._build())).toMatchObject({
      conditions: [{ column: "title", op: "contains", value: "alpha" }],
      orderBy: [["id", "asc"]],
      limit: 26,
      offset: 0,
    });
  });

  it("edits cells in place with selectors and saves from the banner", async () => {
    renderGrid();

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));
    const titleEditor = screen.getByLabelText("Edit title");
    fireEvent.change(titleEditor, { target: { value: "zeta updated" } });
    fireEvent.blur(titleEditor);

    const doneCell = getContainingCell(
      screen.getByRole("checkbox", { name: "Toggle done for row-2" }),
    );
    expect(doneCell).not.toBeNull();
    fireEvent.doubleClick(doneCell as HTMLElement);
    const doneEditor = screen.getByLabelText("Edit done");
    expect(doneEditor.tagName).toBe("SELECT");
    fireEvent.change(doneEditor, { target: { value: "true" } });
    fireEvent.blur(doneEditor);

    expect(screen.getByText("Queued")).not.toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          title: "zeta updated",
          done: true,
        }),
      );
      expect(mockUpdateWait).toHaveBeenCalledWith({ tier: "local" });
    });
  });

  it("opens select-backed editors when edit mode starts", () => {
    const selectPrototype = HTMLSelectElement.prototype as HTMLSelectElement & {
      showPicker?: () => void;
    };
    const originalShowPicker = selectPrototype.showPicker;
    const showPicker = vi.fn();
    selectPrototype.showPicker = showPicker;

    try {
      renderGrid();

      fireEvent.doubleClick(screen.getByRole("gridcell", { name: "open" }));

      expect(screen.getByLabelText("Edit status")).not.toBeNull();
      expect(showPicker).toHaveBeenCalledTimes(1);
    } finally {
      if (originalShowPicker) {
        selectPrototype.showPicker = originalShowPicker;
      } else {
        delete selectPrototype.showPicker;
      }
    }
  });

  it("preserves queued inline edits when the current row live-updates", async () => {
    const { rerender } = renderGrid();

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));
    const editor = screen.getByLabelText("Edit title");
    fireEvent.change(editor, { target: { value: "local draft" } });
    fireEvent.blur(editor);

    currentRows = [{ ...currentRows[0], title: "server pushed update" }, currentRows[1]!];
    rerender(renderGridUi());

    expect(screen.getByText("local draft")).not.toBeNull();
    expect(screen.queryByText("server pushed update")).toBeNull();
  });

  it("sets nullable columns to NULL from the inline editor action", async () => {
    renderGrid();

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: '{"done":true}' }));
    expect(screen.queryByRole("checkbox", { name: "Set meta to NULL" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Set meta to NULL" }));

    await waitFor(() => {
      expect(screen.queryByLabelText("Edit meta")).toBeNull();
    });

    expect(
      within(getContainingRow(screen.getByText("row-2")) as HTMLElement).getByText("<null>"),
    ).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          meta: null,
        }),
      );
      expect(mockUpdateWait).toHaveBeenCalledWith({ tier: "local" });
    });
  });

  it("queues inline cell edits on double click and saves them from the banner", async () => {
    renderGrid();

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));
    const editor = screen.getByLabelText("Edit title");
    fireEvent.change(editor, { target: { value: "zeta queued" } });
    fireEvent.blur(editor);

    expect(screen.getByText("Queued")).not.toBeNull();
    expect(screen.getByText("1 edit across 1 row")).not.toBeNull();
    expect(screen.getByText("zeta queued")).not.toBeNull();
    expect(mockUpdate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          title: "zeta queued",
        }),
      );
      expect(mockUpdateWait).toHaveBeenCalledWith({ tier: "local" });
    });

    expect(screen.queryByText(/queued change across/i)).toBeNull();
  });

  it("renders boolean table cells as always-on checkboxes and saves queued toggles", async () => {
    renderGrid();

    const checkbox = screen.getByRole("checkbox", { name: "Toggle done for row-2" });
    expect((checkbox as HTMLInputElement).checked).toBe(false);

    fireEvent.click(checkbox);

    expect(screen.getByText("Queued")).not.toBeNull();
    expect(screen.getByText("1 edit across 1 row")).not.toBeNull();
    expect(mockUpdate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockUpdate).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
        expect.objectContaining({
          done: true,
        }),
      );
      expect(mockUpdateWait).toHaveBeenCalledWith({ tier: "local" });
    });
  });

  it("does not queue row deletion when backspace is pressed inside an active cell editor", () => {
    render(
      <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
        <TableDataGrid />
      </MemoryRouter>,
    );

    fireEvent.doubleClick(screen.getByRole("gridcell", { name: "zeta" }));

    const editor = screen.getByLabelText("Edit title");
    fireEvent.keyDown(editor, { key: "Backspace" });

    expect(screen.queryByText(/row will be deleted/i)).toBeNull();
    expect(screen.getByLabelText("Edit title")).not.toBeNull();
  });

  it("caps data column width so long cell values do not stretch the whole grid", () => {
    renderGrid();

    const titleMeasuringCell = document.querySelector(
      '[data-measuring-cell-key="title"]',
    ) as HTMLElement | null;
    expect(titleMeasuringCell).not.toBeNull();
    expect(titleMeasuringCell?.style.maxWidth).toBe("360px");
  });

  it("renders without frozen columns so actions stay last and id scrolls normally", () => {
    renderGrid();

    expect(document.querySelector(".rdg-cell-frozen")).toBeNull();
  });

  it("marks changed cells so live updates can pulse", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    currentRows = [{ ...currentRows[0], title: "zeta updated live" }, currentRows[1]!];
    rerender(renderGridUi());

    const changedCell = getContainingCell(screen.getByText("zeta updated live"));
    expect(changedCell?.getAttribute("data-cell-change-state")).toBe("updated");

    act(() => {
      vi.advanceTimersByTime(1_300);
    });

    expect(getContainingCell(screen.getByText("zeta updated live"))?.dataset.cellChangeState).toBe(
      undefined,
    );
  });

  it("highlights rows that were inserted by a live update", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    currentRows = [
      {
        id: "row-3",
        title: "brand new",
        done: false,
        meta: null,
        owner_id: "owner-c",
        blob: null,
      },
      ...currentRows,
    ];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-3"))?.getAttribute("data-row-change-state")).toBe(
      "added",
    );

    act(() => {
      vi.advanceTimersByTime(2_100);
    });

    expect(getContainingRow(screen.getByText("row-3"))?.dataset.rowChangeState).toBe(undefined);
  });

  it("does not highlight rows when the first result set loads", () => {
    currentRows = [];
    const { rerender } = renderGrid();

    currentRows = [
      {
        id: "row-3",
        title: "brand new",
        done: false,
        meta: null,
        owner_id: "owner-c",
        blob: null,
      },
      {
        id: "row-2",
        title: "zeta",
        done: false,
        meta: { done: true },
        owner_id: "owner-a",
        blob: new Uint8Array([1, 2]),
      },
      {
        id: "row-1",
        title: "alpha",
        done: true,
        meta: null,
        owner_id: "owner-b",
        blob: new Uint8Array([5, 6]),
      },
    ];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-3"))?.dataset.rowChangeState).toBe(undefined);
    expect(getContainingRow(screen.getByText("row-2"))?.dataset.rowChangeState).toBe(undefined);
    expect(getContainingRow(screen.getByText("row-1"))?.dataset.rowChangeState).toBe(undefined);
  });

  it("keeps removed rows around briefly so they can animate out", () => {
    vi.useFakeTimers();
    const { rerender } = renderGrid();

    // before: row-2, row-1
    // after:  row-1
    // row-2 should stay rendered long enough to fade out.
    currentRows = [currentRows[1]!];
    rerender(renderGridUi());

    expect(getContainingRow(screen.getByText("row-2"))?.getAttribute("data-row-change-state")).toBe(
      "removed",
    );

    act(() => {
      vi.advanceTimersByTime(700);
    });

    expect(screen.queryByText("row-2")).toBeNull();
  });

  it("appends a staged insert row and inserts it from the banner", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert" }));

    expect(screen.getByText("staged")).not.toBeNull();
    expect(screen.getByText("1 staged insert")).not.toBeNull();
    expect(screen.queryByRole("heading", { name: "Insert row" })).toBeNull();

    const stagedCells = getCellsInRowContaining("staged");

    fireEvent.doubleClick(stagedCells[1] as HTMLElement);
    const titleEditor = screen.getByLabelText("Edit title");
    fireEvent.change(titleEditor, { target: { value: "new todo" } });
    fireEvent.blur(titleEditor);

    fireEvent.doubleClick(stagedCells[2] as HTMLElement);
    const doneEditor = screen.getByLabelText("Edit done");
    expect(doneEditor.tagName).toBe("SELECT");
    fireEvent.change(doneEditor, { target: { value: "true" } });
    fireEvent.blur(doneEditor);

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockInsert).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        expect.objectContaining({
          title: "new todo",
          done: true,
          meta: null,
          owner_id: null,
        }),
      );
      expect(mockInsertWait).toHaveBeenCalledWith({ tier: "local" });
    });
  });

  it("cancels a staged insert row", () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Insert" }));
    expect(screen.getByText("staged")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Cancel staged insert" }));
    expect(screen.queryByText("staged")).toBeNull();
    expect(screen.queryByText("1 staged insert")).toBeNull();
  });

  it("deletes a row when a queued delete is saved", async () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Delete row-2" }));
    expect(screen.getByText("1 row will be deleted")).not.toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(mockDelete).toHaveBeenCalledWith(
        expect.objectContaining({ _table: "todos" }),
        "row-2",
      );
      expect(mockDeleteWait).toHaveBeenCalledWith({ tier: "local" });
    });
  });

  it("does not delete when a queued delete is undone", () => {
    renderGrid();

    fireEvent.click(screen.getByRole("button", { name: "Delete row-2" }));
    fireEvent.click(screen.getByRole("button", { name: "Undo delete row-2" }));

    expect(screen.queryByText("1 row will be deleted")).toBeNull();
    expect(mockDelete).not.toHaveBeenCalled();
  });
});
