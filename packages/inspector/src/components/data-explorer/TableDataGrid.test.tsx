import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TableDataGrid } from "./TableDataGrid";

const mockSubscribeAll = vi.fn();
let currentRows: Array<Record<string, unknown>>;

const mockWasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "meta", column_type: { type: "Row", columns: [] }, nullable: true },
    ],
  },
};

vi.mock("jazz-tools/react", () => ({
  useDb: () => ({
    subscribeAll: (...args: unknown[]) => mockSubscribeAll(...args),
  }),
}));

vi.mock("jazz-tools", () => ({
  allRowsInTableQuery: () => ({
    _table: "todos",
    _schema: {},
    _rowType: undefined,
    _build: () => JSON.stringify({ table: "todos" }),
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
      { id: "row-2", title: "zeta", done: false, meta: { done: true } },
      { id: "row-1", title: "alpha", done: true, meta: null },
    ];

    mockSubscribeAll.mockImplementation((_, callback) => {
      callback({ all: currentRows, delta: [] });
      return vi.fn();
    });
  });

  it("renders schema-derived columns and reactive rows", () => {
    render(<TableDataGrid />);

    expect(screen.getByRole("heading", { name: "todos" })).not.toBeNull();
    expect(screen.getByText("4 columns · 2 rows")).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "ID" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "title" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "done" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "meta" })).not.toBeNull();
    expect(screen.getByText("row-2")).not.toBeNull();
    expect(screen.getByText("zeta")).not.toBeNull();
    expect(screen.getByText('{"done":true}')).not.toBeNull();
  });

  it("sorts rows when a column header is clicked", () => {
    render(<TableDataGrid />);

    const titleHeader = screen.getAllByRole("columnheader", { name: "title" })[0];
    fireEvent.click(titleHeader);

    const renderedRows = screen.getAllByRole("row");
    const firstDataRow = renderedRows[1];
    const firstDataRowCells = within(firstDataRow).getAllByRole("cell");

    expect(firstDataRowCells[1]?.textContent).toBe("alpha");
  });

  it("subscribes with local-only propagation in extension mode", () => {
    render(<TableDataGrid />);

    expect(mockSubscribeAll).toHaveBeenCalledWith(
      expect.any(Object),
      expect.any(Function),
      expect.objectContaining({ propagation: "local-only" }),
    );
  });
});
