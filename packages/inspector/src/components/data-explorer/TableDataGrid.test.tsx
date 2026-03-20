import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";

const mockUseAll = vi.fn();
const mockUpdateDurable = vi.fn();
const mockInsertDurable = vi.fn();
const mockDeleteDurable = vi.fn();
let currentRows: Array<Record<string, unknown>>;

const mockWasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "meta", column_type: { type: "Row", columns: [] }, nullable: true },
      { name: "owner_id", column_type: { type: "Uuid" }, nullable: true, references: "users" },
      { name: "blob", column_type: { type: "Bytea" }, nullable: true },
    ],
  },
};

vi.mock("jazz-tools/react", () => ({
  useAll: (...args: unknown[]) => mockUseAll(...args),
  useDb: () => ({
    updateDurable: (...args: unknown[]) => mockUpdateDurable(...args),
    insertDurable: (...args: unknown[]) => mockInsertDurable(...args),
    deleteDurable: (...args: unknown[]) => mockDeleteDurable(...args),
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

    mockUpdateDurable.mockReset();
    mockInsertDurable.mockReset();
    mockDeleteDurable.mockReset();
    mockUpdateDurable.mockResolvedValue(undefined);
    mockInsertDurable.mockResolvedValue({ id: "new-row" });
    mockDeleteDurable.mockResolvedValue(undefined);
    mockUseAll.mockReset();
    mockUseAll.mockImplementation(() => currentRows);
  });

  it("renders schema-derived columns and reactive rows", () => {
    render(<TableDataGrid />);

    expect(screen.getByRole("heading", { name: "todos" })).not.toBeNull();
    expect(screen.getByText("6 columns · 2 rows on page · 0 filters")).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: /ID/ })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "title" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "done" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "meta" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "Actions" })).not.toBeNull();
    expect(screen.getByText("row-2")).not.toBeNull();
    expect(screen.getByText("zeta")).not.toBeNull();
    expect(screen.getByText('{"done":true}')).not.toBeNull();
  });

  it("updates query sorting when a sortable column header is clicked", () => {
    render(<TableDataGrid />);

    const firstQuery = mockUseAll.mock.calls[0]?.[0] as { _build: () => string };
    expect(JSON.parse(firstQuery._build())).toMatchObject({
      orderBy: [["id", "asc"]],
      limit: 11,
      offset: 0,
    });

    const titleHeader = screen.getByRole("columnheader", { name: "title" });
    fireEvent.click(titleHeader);

    const sortedQuery = mockUseAll.mock.calls.at(-1)?.[0] as { _build: () => string };
    expect(JSON.parse(sortedQuery._build())).toMatchObject({
      orderBy: [["title", "asc"]],
      limit: 11,
      offset: 0,
    });
  });

  it("subscribes with local-only propagation in extension mode", () => {
    render(<TableDataGrid />);

    expect(mockUseAll).toHaveBeenCalledWith(
      expect.any(Object),
      expect.objectContaining({
        propagation: "local-only",
        visibility: "hidden_from_live_query_list",
      }),
    );
  });

  it("adds a where clause and compiles it into query conditions", () => {
    render(<TableDataGrid />);

    fireEvent.click(screen.getByRole("button", { name: /Filter/ }));
    fireEvent.change(screen.getByLabelText("Column"), { target: { value: "title" } });
    fireEvent.change(screen.getByLabelText("Operator"), { target: { value: "contains" } });
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "alpha" } });
    fireEvent.click(screen.getByRole("button", { name: "Add where clause" }));

    const filteredQuery = mockUseAll.mock.calls.at(-1)?.[0] as { _build: () => string };
    expect(JSON.parse(filteredQuery._build())).toMatchObject({
      conditions: [{ column: "title", op: "contains", value: "alpha" }],
      orderBy: [["id", "asc"]],
      limit: 11,
      offset: 0,
    });
  });

  it("opens row edit sidebar and updates editable fields", () => {
    render(<TableDataGrid />);

    fireEvent.click(screen.getAllByRole("button", { name: "Edit" })[0] as Element);

    expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();
    expect(screen.getByText("Read-only: binary field")).not.toBeNull();

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "zeta updated" } });
    fireEvent.change(screen.getByLabelText("done"), { target: { value: "true" } });
    fireEvent.change(screen.getByLabelText("owner_id"), { target: { value: "owner-c" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(mockUpdateDurable).toHaveBeenCalledWith(
      expect.objectContaining({ _table: "todos" }),
      "row-2",
      expect.objectContaining({
        title: "zeta updated",
        done: true,
        owner_id: "owner-c",
      }),
      { tier: "worker" },
    );
  });

  it("preserves unsaved edits when the current row live-updates", () => {
    const { rerender } = render(<TableDataGrid />);

    fireEvent.click(screen.getAllByRole("button", { name: "Edit" })[0] as Element);
    fireEvent.change(screen.getByLabelText("title"), { target: { value: "local draft" } });

    currentRows = [{ ...currentRows[0], title: "server pushed update" }, currentRows[1]!];
    rerender(<TableDataGrid />);

    expect((screen.getByLabelText("title") as HTMLInputElement).value).toBe("local draft");
  });

  it("opens insert sidebar and inserts a new row", () => {
    render(<TableDataGrid />);

    fireEvent.click(screen.getByRole("button", { name: "Insert" }));

    expect(screen.getByRole("heading", { name: "Insert row" })).not.toBeNull();
    expect(screen.getByDisplayValue("auto-generated")).not.toBeNull();

    fireEvent.change(screen.getByLabelText("title"), { target: { value: "new todo" } });
    fireEvent.change(screen.getByLabelText("done"), { target: { value: "true" } });
    fireEvent.click(screen.getAllByRole("button", { name: "Insert" })[1] as Element);

    expect(mockInsertDurable).toHaveBeenCalledWith(
      expect.objectContaining({ _table: "todos" }),
      expect.objectContaining({
        title: "new todo",
        done: true,
        meta: null,
      }),
      { tier: "worker" },
    );
  });

  it("closes sidebar when clicking outside", () => {
    render(<TableDataGrid />);

    fireEvent.click(screen.getAllByRole("button", { name: "Edit" })[0] as Element);
    expect(screen.getByRole("heading", { name: "Edit row" })).not.toBeNull();

    fireEvent.click(screen.getByTestId("row-mutation-overlay"));
    expect(screen.queryByRole("heading", { name: "Edit row" })).toBeNull();
  });

  it("deletes a row when delete is confirmed", () => {
    const confirmSpy = vi.spyOn(globalThis, "confirm").mockReturnValue(true);
    render(<TableDataGrid />);

    fireEvent.click(screen.getAllByRole("button", { name: "Delete" })[0] as Element);

    expect(confirmSpy).toHaveBeenCalledWith('Delete row "row-2" from "todos"?');
    expect(mockDeleteDurable).toHaveBeenCalledWith(
      expect.objectContaining({ _table: "todos" }),
      "row-2",
      { tier: "worker" },
    );
    confirmSpy.mockRestore();
  });

  it("does not delete when delete is canceled", () => {
    const confirmSpy = vi.spyOn(globalThis, "confirm").mockReturnValue(false);
    render(<TableDataGrid />);

    fireEvent.click(screen.getAllByRole("button", { name: "Delete" })[0] as Element);

    expect(mockDeleteDurable).not.toHaveBeenCalled();
    confirmSpy.mockRestore();
  });
});
