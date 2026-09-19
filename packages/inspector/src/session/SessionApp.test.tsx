import { cleanup, act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { PropsWithChildren } from "react";

const mock = vi.hoisted(() => ({
  authorize: vi.fn(),
  renew: vi.fn(),
  logout: vi.fn(),
  create: vi.fn(),
  receive: vi.fn(),
  hashes: vi.fn(),
  schema: vi.fn(),
  permissions: vi.fn(),
}));
vi.mock("./browser-session.js", () => ({
  DashboardInspectorSession: class {
    authorize = mock.authorize;
    renew = mock.renew;
    logout = mock.logout;
  },
  receiveCliSession: mock.receive,
}));
vi.mock("jazz-tools/_dev/inspector-client", () => ({ createInspectorSessionClient: mock.create }));
vi.mock("jazz-tools", () => ({
  fetchSchemaHashes: mock.hashes,
  fetchStoredWasmSchema: mock.schema,
  fetchStoredPermissions: mock.permissions,
}));
vi.mock("jazz-tools/react", () => ({
  JazzClientProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock("../contexts/devtools-context.js", () => ({
  DevtoolsProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock("../contexts/standalone-context.js", () => ({
  StandaloneProvider: ({ children }: PropsWithChildren) => children,
}));
vi.mock("../routes.js", () => ({ InspectorRoutes: () => <p>Protected application rows</p> }));
import SessionApp, { readSessionConnection } from "./SessionApp.js";

const connection = { appId: "app", dashboard: "https://dashboard.example" };
const token = (accessToken = "ephemeral", ttl = 900) => ({
  accessToken,
  appId: "app",
  serverUrl: "https://sync.example",
  expiresAt: Math.floor(Date.now() / 1000) + ttl,
  capabilities: ["inspector:read"],
});
function setup() {
  vi.resetAllMocks();
  mock.authorize.mockResolvedValue(token());
  mock.hashes.mockResolvedValue({ hashes: ["schema"] });
  mock.schema.mockResolvedValue({ schema: {} });
  mock.permissions.mockResolvedValue(null);
  const client = { shutdown: vi.fn().mockResolvedValue(undefined) };
  mock.create.mockResolvedValue(client);
  return client;
}
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  localStorage.clear();
  window.history.replaceState(null, "", "/");
});

describe("Inspector session lifetime", () => {
  it("clears protected rows and shuts down on denied renewal without a popup", async () => {
    const client = setup();
    mock.authorize.mockResolvedValue(token("ephemeral", 61));
    mock.renew.mockRejectedValue(new Error("denied"));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    render(<SessionApp connection={connection} />);
    await act(async () => {
      fireEvent.click(screen.getByText("Sign in with dashboard"));
    });
    expect(screen.getByText("Protected application rows")).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1100);
    });
    expect(screen.queryByText("Protected application rows")).toBeNull();
    expect(screen.getByRole("alert").textContent).toContain("Session ended");
    expect(client.shutdown).toHaveBeenCalledTimes(1);
    expect(mock.authorize).toHaveBeenCalledTimes(1);
    expect(mock.renew).toHaveBeenCalledWith(["inspector:read"]);
  });
  it("disposes a late old client and cannot overwrite a newer session after logout", async () => {
    setup();
    let resolveOld!: (client: { shutdown: ReturnType<typeof vi.fn> }) => void;
    const old = { shutdown: vi.fn().mockResolvedValue(undefined) };
    const newer = { shutdown: vi.fn().mockResolvedValue(undefined) };
    mock.create
      .mockReturnValueOnce(
        new Promise((resolve) => {
          resolveOld = resolve;
        }),
      )
      .mockResolvedValueOnce(newer);
    render(<SessionApp connection={connection} />);
    fireEvent.click(screen.getByText("Sign in with dashboard"));
    await waitFor(() => expect(mock.create).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByText("Log out"));
    fireEvent.click(screen.getByText("Sign in with dashboard"));
    await screen.findByText("Protected application rows");
    await act(async () => {
      resolveOld(old);
    });
    expect(old.shutdown).toHaveBeenCalledTimes(1);
    expect(newer.shutdown).not.toHaveBeenCalled();
    expect(screen.getByText("Protected application rows")).toBeTruthy();
  });
  it("clears at hard expiry even when renewal is still pending", async () => {
    const client = setup();
    mock.authorize.mockResolvedValue(token("ephemeral", 61));
    mock.renew.mockReturnValue(new Promise(() => {}));
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    render(<SessionApp connection={connection} />);
    await act(async () => {
      fireEvent.click(screen.getByText("Sign in with dashboard"));
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(61_001);
    });
    expect(screen.queryByText("Protected application rows")).toBeNull();
    expect(client.shutdown).toHaveBeenCalledTimes(1);
  });
  it("does not restore a CLI handoff that finishes after logout", async () => {
    setup();
    let resolve!: (value: unknown) => void;
    mock.receive.mockReturnValue(
      new Promise((r) => {
        resolve = r;
      }),
    );
    render(<SessionApp connection={{ appId: "app", handoff: "http://127.0.0.1:1234" }} />);
    fireEvent.click(screen.getByText("Log out"));
    await act(async () => {
      resolve(token());
    });
    expect(mock.create).not.toHaveBeenCalled();
    expect(screen.queryByText("Protected application rows")).toBeNull();
  });
  it("persists app metadata only and uses the configured dashboard", () => {
    setup();
    window.history.replaceState(
      null,
      "",
      "/#appId=app&login=dashboard&dashboard=https://other.example&accessToken=must-discard",
    );
    expect(readSessionConnection("https://dashboard.example")).toEqual(connection);
    expect(localStorage.getItem("jazz-inspector-session-connection")).toBe(
      '{"appId":"app","mode":"dashboard"}',
    );
    expect(window.location.hash).toBe("");
  });
});
