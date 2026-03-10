import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LiveQuery } from "./index";

const mockGetActiveQuerySubscriptions = vi.fn();
const mockOnActiveQuerySubscriptionsChange = vi.fn();
const mockUseDevtoolsContext = vi.fn();

vi.mock("jazz-tools", () => ({
  getActiveQuerySubscriptions: () => mockGetActiveQuerySubscriptions(),
  onActiveQuerySubscriptionsChange: (listener: (subscriptions: unknown[]) => void) =>
    mockOnActiveQuerySubscriptionsChange(listener),
}));

vi.mock("../../contexts/devtools-context.js", () => ({
  useDevtoolsContext: () => mockUseDevtoolsContext(),
}));

describe("LiveQuery", () => {
  afterEach(() => {
    cleanup();
  });

  beforeEach(() => {
    mockGetActiveQuerySubscriptions.mockReset();
    mockOnActiveQuerySubscriptionsChange.mockReset();
    mockUseDevtoolsContext.mockReset();
    mockUseDevtoolsContext.mockReturnValue({
      runtime: "extension",
      wasmSchema: {
        projects: { columns: [] },
        todos: { columns: [] },
      },
    });
    mockGetActiveQuerySubscriptions.mockReturnValue([]);
    mockOnActiveQuerySubscriptionsChange.mockImplementation(() => vi.fn());
  });

  it("renders an empty state when there are no active subscriptions", () => {
    render(<LiveQuery />);

    expect(screen.getByText("No active subscriptions")).not.toBeNull();
  });

  it("renders traced subscriptions from the extension bridge", () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "worker",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack:
          "Error\n    at useAll (http://localhost:5173/@fs/.../use-all.js:37:12)\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)\n    at renderWithHooks (http://localhost:5173/node_modules/.vite/deps/react-dom_client.js:5652:24)",
      },
    ]);

    render(<LiveQuery />);

    expect(screen.getByRole("cell", { name: "todos" })).not.toBeNull();
    expect(screen.getByText('{"table":"todos"}')).not.toBeNull();
    const summary = screen.getByText(/TodoList\.tsx:34:17/, { selector: "summary" });
    expect(summary).not.toBeNull();

    fireEvent.click(summary);

    expect(screen.getByText(/at useAll/)).not.toBeNull();
  });

  it("filters rows by table and tier", () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "worker",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack: "Error\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)",
      },
      {
        id: "sub-2",
        table: "projects",
        tier: "edge",
        branches: ["main"],
        createdAt: "2026-03-10T11:00:00.000Z",
        query: '{"table":"projects"}',
        stack: "Error\n    at ProjectList (http://localhost:5173/src/ProjectList.tsx:10:8)",
      },
    ]);

    render(<LiveQuery />);

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

  it("sorts by started date and tier, and allows toggling started order", () => {
    mockGetActiveQuerySubscriptions.mockReturnValue([
      {
        id: "sub-1",
        table: "todos",
        tier: "global",
        branches: ["main"],
        createdAt: "2026-03-10T10:00:00.000Z",
        query: '{"table":"todos"}',
        stack: "Error\n    at TodoList (http://localhost:5173/src/TodoList.tsx:34:17)",
      },
      {
        id: "sub-2",
        table: "projects",
        tier: "edge",
        branches: ["main"],
        createdAt: "2026-03-10T11:00:00.000Z",
        query: '{"table":"projects"}',
        stack: "Error\n    at ProjectList (http://localhost:5173/src/ProjectList.tsx:10:8)",
      },
      {
        id: "sub-3",
        table: "users",
        tier: "worker",
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

    render(<LiveQuery />);

    const rows = screen.getAllByRole("row");
    expect(rows[1]?.textContent).toContain("users");
    expect(rows[2]?.textContent).toContain("projects");
    expect(rows[3]?.textContent).toContain("todos");

    fireEvent.click(screen.getByRole("columnheader", { name: /Started/ }));

    const reSortedRows = screen.getAllByRole("row");
    expect(reSortedRows[1]?.textContent).toContain("todos");
  });
});
