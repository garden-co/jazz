import { cleanup, fireEvent, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithRouter } from "../../test/renderWithRouter";
import { InspectorLayout } from "./index";

const mockUseStandaloneContext = vi.fn();
const mockUseDevtoolsContext = vi.fn();
const routeParams = { branch: "main", connectionId: "local", schemaHash: "hash-a" };

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

  it("shows schema dropdown and manage button when standalone context is available", async () => {
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

    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    expect(await screen.findByRole("button", { name: "Connections" })).not.toBeNull();
    expect(screen.getByRole("combobox")).not.toBeNull();
    expect(screen.getByRole("option", { name: "hash-a" })).not.toBeNull();
    expect(screen.getByRole("option", { name: "hash-b" })).not.toBeNull();
    expect(screen.getByRole("link", { name: "Live Query" })).not.toBeNull();
  });

  it("calls manage handler when connections button is clicked", async () => {
    const onManageConnections = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections,
      onReset: vi.fn(),
      schemaHashes: ["hash-a"],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: false,
    });

    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    fireEvent.click(await screen.findByRole("button", { name: "Connections" }));

    expect(onManageConnections).toHaveBeenCalledTimes(1);
  });

  it("calls schema selection handler when dropdown value changes", async () => {
    const onSelectSchema = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: ["hash-a", "hash-b"],
      selectedSchemaHash: "hash-a",
      onSelectSchema,
      isSwitchingSchema: false,
    });

    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    fireEvent.change(await screen.findByRole("combobox"), { target: { value: "hash-b" } });

    expect(onSelectSchema).toHaveBeenCalledWith("hash-b");
  });

  it("disables schema dropdown while switching or when no schemas are available", async () => {
    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: ["hash-a"],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: true,
    });

    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    expect((await screen.findByRole("combobox")).hasAttribute("disabled")).toBe(true);

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: [],
      selectedSchemaHash: "hash-a",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: false,
    });

    cleanup();
    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    expect((await screen.findByRole("combobox")).hasAttribute("disabled")).toBe(true);
  });

  it("hides schema actions when config reset context is unavailable", () => {
    mockUseStandaloneContext.mockReturnValue(null);

    renderWithRouter(<InspectorLayout routeParams={routeParams} />);

    expect(screen.queryByRole("button", { name: "Connections" })).toBeNull();
    expect(screen.queryByRole("combobox")).toBeNull();
  });
});
