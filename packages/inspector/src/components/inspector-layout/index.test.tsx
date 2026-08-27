import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { SettingsPage } from "../../pages/settings/index.js";
import {
  DETACH_SHORTCUT_TOOLTIP,
  OVERLAY_ROUTE_MESSAGE_TYPE,
  OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY,
} from "../../utility/overlay-settings.js";
import { InspectorLayout } from "./index";

const mockUseStandaloneContext = vi.fn();
const mockUseDevtoolsContext = vi.fn();
const mockIsDetachedInspector = vi.fn();
const mockRequestDetachOverlay = vi.fn();

function LocationProbe() {
  return <p>{useLocation().pathname}</p>;
}

vi.mock("../../contexts/standalone-context.js", () => ({
  useStandaloneContext: () => mockUseStandaloneContext(),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

vi.mock("../../utility/overlay-settings.js", () => ({
  DETACH_SHORTCUT_KEYS: ["Alt", "Shift", "D"],
  DETACH_SHORTCUT_TOOLTIP: "Open in separate window Alt+Shift+D",
  OVERLAY_ROUTE_MESSAGE_TYPE: "jazz-inspector-overlay:route",
  OVERLAY_HIDE_LAUNCHER_STORAGE_KEY: "jazz-inspector-overlay:hide-toggle",
  OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY: "jazz-inspector-overlay:show-detach-button",
  isDetachedInspector: () => mockIsDetachedInspector(),
  isBoolean: (value: unknown) => typeof value === "boolean",
  requestCloseOverlay: vi.fn(),
  requestDetachOverlay: (...args: unknown[]) => mockRequestDetachOverlay(...args),
  setOverlayActiveRoute: vi.fn(),
}));

describe("InspectorLayout", () => {
  beforeEach(() => {
    localStorage.clear();
    mockUseStandaloneContext.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockIsDetachedInspector.mockReset();
    mockRequestDetachOverlay.mockReset();
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
    expect(screen.getByText(DETACH_SHORTCUT_TOOLTIP)).not.toBeNull();

    fireEvent.keyDown(window, { altKey: true, shiftKey: true, code: "KeyD" });
    expect(mockRequestDetachOverlay).toHaveBeenNthCalledWith(2, "/data-explorer/todos/data");
  });

  it("shows the detach button by default and lets settings hide it", () => {
    mockUseStandaloneContext.mockReturnValue(null);
    mockUseDevtoolsContext.mockReturnValue({ runtime: "overlay" });

    render(
      <MemoryRouter initialEntries={["/settings"]}>
        <Routes>
          <Route element={<InspectorLayout />}>
            <Route path="settings" element={<SettingsPage />} />
          </Route>
        </Routes>
      </MemoryRouter>,
    );

    const visibility = screen.getByRole("switch", { name: "Show the detach button" });
    expect(
      screen.getByRole("button", { name: "Open inspector in separate window" }),
    ).not.toBeNull();

    fireEvent.click(visibility);

    expect(screen.queryByRole("button", { name: "Open inspector in separate window" })).toBeNull();
    expect(localStorage.getItem(OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY)).toBe("false");
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

  it("restores the dock to the route received from the overlay chrome", () => {
    mockUseStandaloneContext.mockReturnValue(null);
    mockUseDevtoolsContext.mockReturnValue({ runtime: "overlay" });

    render(
      <MemoryRouter initialEntries={["/settings"]}>
        <Routes>
          <Route element={<InspectorLayout />}>
            <Route path="settings" element={<LocationProbe />} />
            <Route path="data-explorer/projects/data" element={<LocationProbe />} />
          </Route>
        </Routes>
      </MemoryRouter>,
    );

    fireEvent(
      window,
      new MessageEvent("message", {
        origin: window.location.origin,
        source: window.parent,
        data: { type: OVERLAY_ROUTE_MESSAGE_TYPE, route: "/data-explorer/projects/data" },
      }),
    );

    expect(screen.getByText("/data-explorer/projects/data")).not.toBeNull();
  });

  it("rejects route messages from a spoofed source or origin", () => {
    mockUseStandaloneContext.mockReturnValue(null);
    mockUseDevtoolsContext.mockReturnValue({ runtime: "overlay" });

    render(
      <MemoryRouter initialEntries={["/settings"]}>
        <Routes>
          <Route element={<InspectorLayout />}>
            <Route path="settings" element={<LocationProbe />} />
            <Route path="data-explorer/projects/data" element={<LocationProbe />} />
          </Route>
        </Routes>
      </MemoryRouter>,
    );

    fireEvent(
      window,
      new MessageEvent("message", {
        origin: window.location.origin,
        source: {} as MessageEventSource,
        data: { type: OVERLAY_ROUTE_MESSAGE_TYPE, route: "/data-explorer/projects/data" },
      }),
    );
    expect(screen.getByText("/settings")).not.toBeNull();

    fireEvent(
      window,
      new MessageEvent("message", {
        origin: "https://untrusted.example",
        source: window.parent,
        data: { type: OVERLAY_ROUTE_MESSAGE_TYPE, route: "/data-explorer/projects/data" },
      }),
    );
    expect(screen.getByText("/settings")).not.toBeNull();
  });
});
