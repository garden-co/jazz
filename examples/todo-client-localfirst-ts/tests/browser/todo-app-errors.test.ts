import { afterEach, describe, expect, it, vi } from "vitest";

const jazz = vi.hoisted(() => ({
  createDb: vi.fn(),
  getSecret: vi.fn().mockResolvedValue("test-secret"),
}));

vi.mock("jazz-tools", async (importActual) => ({
  ...(await importActual<typeof import("jazz-tools")>()),
  createDb: jazz.createDb,
  BrowserAuthSecretStore: { getOrCreateSecret: jazz.getSecret },
}));

import { startApp } from "../../src/main.js";

function readyDb(overrides: Record<string, unknown> = {}) {
  return {
    all: vi.fn().mockResolvedValue([]),
    getAuthState: () => ({ session: { user: "test-user" } }),
    subscribe: (_query: unknown, listener: (rows: []) => void) => {
      listener([]);
      return vi.fn();
    },
    onAuthChanged: () => vi.fn(),
    onMutationError: () => vi.fn(),
    shutdown: vi.fn().mockResolvedValue(undefined),
    insert: vi.fn(),
    update: vi.fn(),
    delete: vi.fn(),
    ...overrides,
  };
}

describe("todo app local receipts", () => {
  let container: HTMLDivElement;

  afterEach(() => {
    container?.remove();
    jazz.createDb.mockReset();
  });

  it("rejects startup and shuts down when the initial local read fails", async () => {
    container = document.createElement("div");
    const failure = Object.assign(new Error("worker attachment failed"), {
      name: "WorkerAttachmentError",
      code: "worker_unavailable",
    });
    const db = readyDb({
      all: vi.fn().mockRejectedValue(failure),
      shutdown: vi.fn().mockRejectedValue(new Error("shutdown unavailable")),
    });
    jazz.createDb.mockResolvedValue(db);

    await expect(startApp(container, { appId: "test-app", secret: "test-secret" })).rejects.toBe(
      failure,
    );
    expect(db.shutdown).toHaveBeenCalledOnce();
    expect(container.innerHTML).toBe("");
  });

  it("keeps form input and exposes a safe code when a local write rejects", async () => {
    container = document.createElement("div");
    const failure = Object.assign(new Error("policy reason must not reach the page"), {
      name: "PersistedWriteRejectedError",
      code: "permission_denied",
    });
    const write = { wait: vi.fn().mockRejectedValue(failure) };
    const db = readyDb({ insert: vi.fn().mockReturnValue(write) });
    jazz.createDb.mockResolvedValue(db);

    const { destroy } = await startApp(container, { appId: "test-app", secret: "test-secret" });
    const input = container.querySelector<HTMLInputElement>("#title-input")!;
    const form = container.querySelector<HTMLFormElement>("#add-form")!;
    input.value = "Keep this task";
    form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

    await vi.waitFor(() => {
      expect(container.querySelector("#mutation-status")?.textContent).toBe("Save failed");
    });
    expect(input.value).toBe("Keep this task");
    expect(container.querySelector("#error-message")?.textContent).toBe(
      "Save failed: PersistedWriteRejectedError (permission_denied): policy reason must not reach the page. Edit and submit again.",
    );
    expect(
      container.querySelector<HTMLButtonElement>('#add-form button[type="submit"]')?.disabled,
    ).toBe(false);
    await destroy();
  });

  it("reports pending until the local receipt resolves, then confirms and clears the form", async () => {
    container = document.createElement("div");
    let resolveReceipt: (() => void) | undefined;
    const write = {
      wait: vi.fn(
        () =>
          new Promise<void>((resolve) => {
            resolveReceipt = resolve;
          }),
      ),
    };
    jazz.createDb.mockResolvedValue(readyDb({ insert: vi.fn().mockReturnValue(write) }));

    const { destroy } = await startApp(container, { appId: "test-app", secret: "test-secret" });
    const input = container.querySelector<HTMLInputElement>("#title-input")!;
    input.value = "Wait for local receipt";
    container
      .querySelector<HTMLFormElement>("#add-form")!
      .dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));

    expect(container.querySelector("#mutation-status")?.textContent).toBe(
      "Saving locally (pending)…",
    );
    expect(input.value).toBe("Wait for local receipt");
    resolveReceipt?.();
    await vi.waitFor(() => {
      expect(container.querySelector("#mutation-status")?.textContent).toBe("Saved locally");
    });
    expect(input.value).toBe("");
    await destroy();
  });
});
