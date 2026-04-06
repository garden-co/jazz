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

vi.mock("./pages/schema-explorer", () => ({
  SchemaExplorer: () => (
    <div>
      schema explorer
      <Outlet />
    </div>
  ),
}));

vi.mock("./pages/schema-explorer/SchemaCompatibilityView", () => ({
  SchemaCompatibilityView: () => <div>compatibility view</div>,
}));

vi.mock("./pages/schema-explorer/SchemaComparisonView", () => ({
  SchemaComparisonView: () => <div>comparison view</div>,
}));

vi.mock("./pages/schema-explorer/SingleSchemaView", () => ({
  SingleSchemaView: () => <div>single schema view</div>,
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

  it("exposes schema compatibility and comparison routes", () => {
    mockUseDevtoolsContext.mockReturnValue({ runtime: "standalone" });

    render(
      <MemoryRouter initialEntries={["/schemas/compatibility"]}>
        <InspectorRoutes />
      </MemoryRouter>,
    );

    expect(screen.getByText("schema explorer")).not.toBeNull();
    expect(screen.getByText("compatibility view")).not.toBeNull();
  });
});
