import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveQuery } from "./index";

const mockFetchServerSubscriptions = vi.fn();
const mockUseDevtoolsContext = vi.fn();
const mockUseStandaloneContext = vi.fn();

vi.mock("jazz-tools", () => ({
  fetchServerSubscriptions: (...args: unknown[]) => mockFetchServerSubscriptions(...args),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

vi.mock("../../contexts/standalone-context.js", () => ({
  useStandaloneContext: () => mockUseStandaloneContext(),
}));

// Overlay subscriptions arrive (stack-less) via the host push, surfaced on the
// devtools context as `hostSubscriptions`.
function overlayContext(hostSubscriptions: unknown[], extraTables: string[] = []) {
  return {
    runtime: "overlay",
    wasmSchema: Object.fromEntries(
      ["projects", "todos", ...extraTables].map((t) => [t, { columns: [] }]),
    ),
    hostSubscriptions,
  };
}

describe("LiveQuery", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockFetchServerSubscriptions.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockUseStandaloneContext.mockReset();
    mockUseDevtoolsContext.mockReturnValue(overlayContext([]));
    mockUseStandaloneContext.mockReturnValue({
      connection: {
        serverUrl: "http://localhost:1625",
        appId: "test-app",
        adminSecret: "admin-secret",
      },
    });
    mockFetchServerSubscriptions.mockResolvedValue({
      appId: "test-app",
      generatedAt: 1741600800000,
      queries: [],
    });
  });

  it("renders an empty state when there are no active overlay subscriptions", () => {
    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    expect(screen.getByText("No active subscriptions")).not.toBeNull();
  });

  it("renders subscriptions pushed from the host (no stack column)", async () => {
    mockUseDevtoolsContext.mockReturnValue(
      overlayContext([
        {
          id: "sub-1",
          table: "todos",
          tier: "edge",
          propagation: "full",
          branches: ["main"],
          createdAt: "2026-03-10T10:00:00.000Z",
          query: '{"table":"todos"}',
        },
      ]),
    );

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("cell", { name: "todos" })).not.toBeNull();
    expect(await screen.findByRole("cell", { name: "full" })).not.toBeNull();
    expect(await screen.findByText('{"table":"todos"}')).not.toBeNull();
    // No stack column anymore.
    expect(screen.queryByRole("columnheader", { name: "Stack" })).toBeNull();
  });

  it("filters overlay rows by table and tier", () => {
    mockUseDevtoolsContext.mockReturnValue(
      overlayContext([
        {
          id: "sub-1",
          table: "todos",
          tier: "local",
          propagation: "full",
          branches: ["main"],
          createdAt: "2026-03-10T10:00:00.000Z",
          query: '{"table":"todos"}',
        },
        {
          id: "sub-2",
          table: "projects",
          tier: "edge",
          propagation: "local-only",
          branches: ["main"],
          createdAt: "2026-03-10T11:00:00.000Z",
          query: '{"table":"projects"}',
        },
      ]),
    );

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByLabelText("Filter by table"), { target: { value: "projects" } });
    expect(screen.queryByRole("cell", { name: "todos" })).toBeNull();
    expect(screen.getByRole("cell", { name: "projects" })).not.toBeNull();

    fireEvent.change(screen.getByLabelText("Filter by tier"), { target: { value: "local" } });
    expect(screen.getByText("No active subscriptions")).not.toBeNull();
  });

  it("sorts overlay rows by started date and tier, and toggles started order", () => {
    mockUseDevtoolsContext.mockReturnValue(
      overlayContext(
        [
          {
            id: "sub-1",
            table: "todos",
            tier: "global",
            propagation: "full",
            branches: ["main"],
            createdAt: "2026-03-10T10:00:00.000Z",
            query: '{"table":"todos"}',
          },
          {
            id: "sub-2",
            table: "projects",
            tier: "edge",
            propagation: "local-only",
            branches: ["main"],
            createdAt: "2026-03-10T11:00:00.000Z",
            query: '{"table":"projects"}',
          },
          {
            id: "sub-3",
            table: "users",
            tier: "local",
            propagation: "full",
            branches: ["main"],
            createdAt: "2026-03-10T11:00:00.000Z",
            query: '{"table":"users"}',
          },
        ],
        ["users"],
      ),
    );

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    const rows = screen.getAllByRole("row");
    expect(rows[1]?.textContent).toContain("users");
    expect(rows[2]?.textContent).toContain("projects");
    expect(rows[3]?.textContent).toContain("todos");

    fireEvent.click(screen.getByRole("columnheader", { name: /Started/ }));

    const reSortedRows = screen.getAllByRole("row");
    expect(reSortedRows[1]?.textContent).toContain("todos");
  });

  it("fetches and renders grouped standalone server telemetry", async () => {
    mockUseDevtoolsContext.mockReturnValue({ runtime: "standalone", wasmSchema: {} });
    mockFetchServerSubscriptions.mockResolvedValue({
      appId: "test-app",
      generatedAt: 1741600800000,
      queries: [
        {
          groupKey: "group-1",
          count: 2,
          table: "todos",
          propagation: "full",
          branches: ["main"],
          query: '{"table":"todos"}',
        },
      ],
    });

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByRole("cell", { name: "todos" })).not.toBeNull();
    });

    expect(mockFetchServerSubscriptions).toHaveBeenCalledWith("http://localhost:1625", {
      adminSecret: "admin-secret",
      appId: "test-app",
    });
    expect(screen.getByRole("cell", { name: "2" })).not.toBeNull();
    expect(screen.queryByLabelText("Filter by tier")).toBeNull();
  });
});
