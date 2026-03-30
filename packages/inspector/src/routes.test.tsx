import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Outlet } from "react-router";
import { InspectorRoutes } from "./routes";

const mockUseDevtoolsContext = vi.fn();

vi.mock("./contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

vi.mock("./components/inspector-layout", () => ({
  InspectorLayout: () => (
    <div>
      layout
      <Outlet />
    </div>
  ),
}));

vi.mock("./pages/data-explorer", () => ({
  DataExplorer: () => <div>data explorer</div>,
}));

vi.mock("./components/data-explorer/TableDataGrid", () => ({
  TableDataGrid: () => <div>table grid</div>,
}));

vi.mock("./components/data-explorer/TableSchemaDefinition", () => ({
  TableSchemaDefinition: () => <div>schema definition</div>,
}));

vi.mock("./pages/live-query", () => ({
  LiveQuery: () => <div>live query page</div>,
}));

describe("InspectorRoutes", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockUseDevtoolsContext.mockReset();
  });

  it("exposes the live query route in extension mode", () => {
    mockUseDevtoolsContext.mockReturnValue({ runtime: "extension" });

    render(
      <MemoryRouter initialEntries={["/live-query"]}>
        <InspectorRoutes />
      </MemoryRouter>,
    );

    expect(screen.getByText("live query page")).not.toBeNull();
  });
});
