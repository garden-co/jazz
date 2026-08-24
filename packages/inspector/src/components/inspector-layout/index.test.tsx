import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter } from "react-router";
import { InspectorLayout } from "./index";

const mockUseStandaloneContext = vi.fn();
const mockUseDevtoolsContext = vi.fn();
const mockIsDetachedInspector = vi.fn();
const mockRequestDetachOverlay = vi.fn();

vi.mock("../../contexts/standalone-context.js", () => ({
  useStandaloneContext: () => mockUseStandaloneContext(),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

vi.mock("../../utility/overlay-settings.js", () => ({
  isDetachedInspector: () => mockIsDetachedInspector(),
  requestCloseOverlay: vi.fn(),
  requestDetachOverlay: (...args: unknown[]) => mockRequestDetachOverlay(...args),
}));

describe("InspectorLayout", () => {
  beforeEach(() => {
    mockUseStandaloneContext.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockIsDetachedInspector.mockReset();
    mockIsDetachedInspector.mockReturnValue(false);
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
      schemaHashes: [
        { hash: "hash-a", publishedAt: null },
        { hash: "hash-b", publishedAt: null },
      ],
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
    expect(screen.getByRole("link", { name: "Subscriptions" })).not.toBeNull();
  });

  it("shortens schema hashes and includes upload time when available", () => {
    mockUseStandaloneContext.mockReturnValue({
      onManageConnections: vi.fn(),
      onReset: vi.fn(),
      schemaHashes: [
        {
          hash: "aaaaaaaaaaaabbbbbbbbbbbbccccccccccccddddddddddddeeeeeeeeeeeeffff",
          publishedAt: Date.UTC(2026, 5, 18, 19, 15),
        },
      ],
      selectedSchemaHash: "aaaaaaaaaaaabbbbbbbbbbbbccccccccccccddddddddddddeeeeeeeeeeeeffff",
      onSelectSchema: vi.fn(),
      isSwitchingSchema: false,
    });

    render(
      <MemoryRouter initialEntries={["/data-explorer"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.getByRole("option", { name: /aaaaaaaaaaaa - uploaded / })).not.toBeNull();
  });

  it("calls manage handler when connections button is clicked", () => {
    const onManageConnections = vi.fn();

    mockUseStandaloneContext.mockReturnValue({
      onManageConnections,
      onReset: vi.fn(),
      schemaHashes: [{ hash: "hash-a", publishedAt: null }],
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
      schemaHashes: [
        { hash: "hash-a", publishedAt: null },
        { hash: "hash-b", publishedAt: null },
      ],
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
      schemaHashes: [{ hash: "hash-a", publishedAt: null }],
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

  it("opens the active route in a separate window from overlay mode", () => {
    mockUseStandaloneContext.mockReturnValue(null);
    mockUseDevtoolsContext.mockReturnValue({ runtime: "overlay" });

    render(
      <MemoryRouter initialEntries={["/data-explorer/todos/data"]}>
        <InspectorLayout />
      </MemoryRouter>,
    );

    const detach = screen.getByRole("button", { name: "Open inspector in separate window" });
    fireEvent.click(detach);
    expect(mockRequestDetachOverlay).toHaveBeenCalledWith("/data-explorer/todos/data");
  });

  it("hides overlay window actions in a detached inspector", () => {
    mockUseStandaloneContext.mockReturnValue(null);
    mockUseDevtoolsContext.mockReturnValue({ runtime: "overlay" });
    mockIsDetachedInspector.mockReturnValue(true);

    render(
      <MemoryRouter>
        <InspectorLayout />
      </MemoryRouter>,
    );

    expect(screen.queryByRole("button", { name: "Open inspector in separate window" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Close inspector" })).toBeNull();
  });
});
