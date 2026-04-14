import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveQuery } from "./index";

const mockFetchServerSubscriptions = vi.fn();
const mockGetActiveQuerySubscriptions = vi.fn();
const mockOnActiveQuerySubscriptionsChange = vi.fn();
const mockUseDevtoolsContext = vi.fn();
const mockUseStandaloneContext = vi.fn();

vi.mock("jazz-tools", () => ({
  fetchServerSubscriptions: (...args: unknown[]) => mockFetchServerSubscriptions(...args),
  getActiveQuerySubscriptions: () => mockGetActiveQuerySubscriptions(),
  onActiveQuerySubscriptionsChange: (listener: (subscriptions: unknown[]) => void) =>
    mockOnActiveQuerySubscriptionsChange(listener),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

vi.mock("../../contexts/standalone-context.js", () => ({
  useStandaloneContext: () => mockUseStandaloneContext(),
}));

describe("LiveQuery", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockFetchServerSubscriptions.mockReset();
    mockGetActiveQuerySubscriptions.mockReset();
    mockOnActiveQuerySubscriptionsChange.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockUseStandaloneContext.mockReset();
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "extension",
      wasmSchema: {
        projects: { columns: [] },
        todos: { columns: [] },
      },
    });
    mockUseStandaloneContext.mockReturnValue({
      connection: {
        serverUrl: "http://localhost:1625",
        appId: "test-app",
        adminSecret: "admin-secret",
      },
    });
    mockGetActiveQuerySubscriptions.mockReturnValue([]);
    mockOnActiveQuerySubscriptionsChange.mockImplementation(() => vi.fn());
    mockFetchServerSubscriptions.mockResolvedValue({
      appId: "test-app",
      generatedAt: 1741600800000,
      queries: [],
    });
  });

  it("renders an empty state when there are no active extension subscriptions", () => {
    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    expect(screen.getByText("No active subscriptions")).not.toBeNull();
  });

  it("renders traced subscriptions from the extension bridge", async () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "worker",
        propagation: "full",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack:
          "Error\n    at useAll (http://localhost:5173/@fs/.../use-all.js:37:12)\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)\n    at renderWithHooks (http://localhost:5173/node_modules/.vite/deps/react-dom_client.js:5652:24)",
      },
    ]);

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("cell", { name: "todos" })).not.toBeNull();
    expect(await screen.findByRole("cell", { name: "full" })).not.toBeNull();
    expect(await screen.findByText('{"table":"todos"}')).not.toBeNull();
    const summary = await screen.findByText(/TodoList\.tsx:34:17/, { selector: "summary" });
    expect(summary).not.toBeNull();

    fireEvent.click(summary);

    expect(await screen.findByText(/at useAll/)).not.toBeNull();
  }, 15_000);

  it("filters extension rows by table and tier", () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "worker",
        propagation: "full",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack: "Error\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)",
      },
      {
        id: "sub-2",
        table: "projects",
        tier: "edge",
        propagation: "local-only",
        branches: ["main"],
        createdAt: "2026-03-10T11:00:00.000Z",
        query: '{"table":"projects"}',
        stack: "Error\n    at ProjectList (http://localhost:5173/src/ProjectList.tsx:10:8)",
      },
    ]);

    render(
      <MemoryRouter>
        <LiveQuery />
      </MemoryRouter>,
    );

    fireEvent.change(screen.getByLabelText("Filter by table"), {
      target: { value: "projects" },
    });

    expect(screen.queryByRole("cell", { name: "todos" })).toBeNull();
    expect(screen.getByRole("cell", { name: "projects" })).not.toBeNull();

    fireEvent.change(screen.getByLabelText("Filter by tier"), {
      target: { value: "worker" },
    });

    expect(screen.getByText("No active subscriptions")).not.toBeNull();
  });

  it("sorts extension rows by started date and tier, and allows toggling started order", () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "global",
        propagation: "full",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack: "Error\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)",
      },
      {
        id: "sub-2",
        table: "projects",
        tier: "edge",
        propagation: "local-only",
        branches: ["main"],
        createdAt: "2026-03-10T11:00:00.000Z",
        query: '{"table":"projects"}',
        stack: "Error\n    at ProjectList (http://localhost:5173/src/ProjectList.tsx:10:8)",
      },
      {
        id: "sub-3",
        table: "users",
        tier: "worker",
        propagation: "full",
        branches: ["main"],
        createdAt: "2026-03-10T11:00:00.000Z",
        query: '{"table":"users"}',
        stack: "Error\n    at UserList (http://localhost:5173/src/UserList.tsx:10:8)",
      },
    ]);
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "extension",
      wasmSchema: {
        projects: { columns: [] },
        todos: { columns: [] },
        users: { columns: [] },
      },
    });

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
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "standalone",
      wasmSchema: {},
    });
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
      pathPrefix: undefined,
    });
    expect(screen.getByRole("cell", { name: "2" })).not.toBeNull();
    expect(screen.queryByLabelText("Filter by tier")).toBeNull();
  });
});
