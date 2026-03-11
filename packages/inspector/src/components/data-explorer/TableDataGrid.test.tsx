import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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
    expect(screen.getByText("4 columns · 2 rows on page")).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: /ID/ })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "title" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "done" })).not.toBeNull();
    expect(screen.getByRole("columnheader", { name: "meta" })).not.toBeNull();
    expect(screen.getByText("row-2")).not.toBeNull();
    expect(screen.getByText("zeta")).not.toBeNull();
    expect(screen.getByText('{"done":true}')).not.toBeNull();
  });

  it("updates query sorting when a sortable column header is clicked", () => {
    render(<TableDataGrid />);

    const firstQuery = mockSubscribeAll.mock.calls[0]?.[0] as { _build: () => string };
    expect(JSON.parse(firstQuery._build())).toMatchObject({
      orderBy: [["id", "asc"]],
      limit: 11,
      offset: 0,
    });

    const titleHeader = screen.getByRole("columnheader", { name: "title" });
    fireEvent.click(titleHeader);

    const sortedQuery = mockSubscribeAll.mock.calls.at(-1)?.[0] as { _build: () => string };
    expect(JSON.parse(sortedQuery._build())).toMatchObject({
      orderBy: [["title", "asc"]],
      limit: 11,
      offset: 0,
    });
  });

  it("subscribes with local-only propagation in extension mode", () => {
    render(<TableDataGrid />);

    expect(mockSubscribeAll).toHaveBeenCalledWith(
      expect.any(Object),
      expect.any(Function),
      expect.objectContaining({
        propagation: "local-only",
        visibility: "hidden_from_live_query_list",
      }),
    );
  });
});
