import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { DataExplorer } from "./index";

const mockSetQueryPropagation = vi.fn();
const mockUseDevtoolsContext = vi.fn();

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

describe("DataExplorer", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockSetQueryPropagation.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockUseDevtoolsContext.mockReturnValue({
      wasmSchema: {
        todos: { columns: [] },
        users: { columns: [] },
      },
      runtime: "extension",
      queryPropagation: "local-only",
      setQueryPropagation: mockSetQueryPropagation,
    });
  });

  it("renders a resizable table list panel", () => {
    render(
      <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
        <Routes>
          <Route path="/data-explorer/:table/*" element={<DataExplorer />}>
            <Route path="data" element={<div>table content</div>} />
          </Route>
        </Routes>
      </MemoryRouter>,
    );

    expect(screen.getAllByRole("separator")).toHaveLength(1);
    expect(screen.getByText("table content")).not.toBeNull();
  });
});
