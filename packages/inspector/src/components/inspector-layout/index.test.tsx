import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { InspectorLayout } from "./index";

const mockUseStandaloneContext = vi.fn();
const mockUseDevtoolsContext = vi.fn();

vi.mock("../../contexts/standalone-context.js", () => ({
  useStandaloneContext: () => mockUseStandaloneContext(),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

describe("InspectorLayout", () => {
  beforeEach(() => {
    mockUseStandaloneContext.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockUseDevtoolsContext.mockReturnValue({ runtime: "extension" });
  });

  afterEach(() => {
    cleanup();
  });

  it("shows schema dropdown and manage button when standalone context is available", () => {
    const onManageConnections = vi.fn();
    const onSelectSchema = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections,
      onReset: vi.fn(),
      schemaHashes: ["hash-a", "hash-b"],
      selectedSchemaHash: "hash-a",
      onSelectSchema,
      isSwitchingSchema: false,
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.getByRole("button", { name: "Connections" })).not.toBeNull();
    expect(screen.getByRole("combobox")).not.toBeNull();
    expect(screen.getByRole("option", { name: "hash-a" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "hash-b" })).not.toBeNull();
    expect(screen.getByRole("link", { name: "Live Query" })).not.toBeNull();
  });

  it("calls manage handler when connections button is clicked", () => {
    const onManageConnections = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections,
      onReset: vi.fn(),
      schemaHashes: ["hash-a"],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: false,
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    fireEvent.click(screen.getByRole("button", { name: "Connections" }));

    expect(onManageConnections).toHaveBeenCalledTimes(1);
  });

  it("calls schema selection handler when dropdown value changes", () => {
    const onSelectSchema = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: ["hash-a", "hash-b"],
      selectedSchemaHash: "hash-a",
      onSelectSchema,
      isSwitchingSchema: false,
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByRole("combobox"), { target: { value: "hash-b" } });

    expect(onSelectSchema).toHaveBeenCalledWith("hash-b");
  });

  it("disables schema dropdown while switching or when no schemas are available", () => {
    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: ["hash-a"],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: true,
    });

    const { rerender } = render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.getByRole("combobox").hasAttribute("disabled")).toBe(true);

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: [],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: false,
    });

    rerender(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.getByRole("combobox").hasAttribute("disabled")).toBe(true);
  });

  it("hides schema actions when config reset context is unavailable", () => {
    mockUseStandaloneContext.mockReturnValue(null);

    render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.queryByRole("button", { name: "Connections" })).toBeNull();
    expect(screen.queryByRole("combobox")).toBeNull();
  });
});
