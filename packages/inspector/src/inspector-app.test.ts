import { act, cleanup, render } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "jazz-tools";
import { defaultRuntimeContextKey } from "./contexts/default-runtime-context";
import type { InspectorRuntimeContext } from "./contexts/host-link";
import { InspectorApp } from "./inspector-app";

const { closePortMock, openSessionMock, readHostConfigMock } = vi.hoisted(() => ({
  closePortMock: vi.fn(),
  openSessionMock: vi.fn(),
  readHostConfigMock: vi.fn(),
}));

vi.mock("./contexts/host-link", () => ({
  closeInspectorRuntimePort: closePortMock,
  openInspectorRuntimeSession: openSessionMock,
  readInspectorHostConfig: readHostConfigMock,
}));

vi.mock("jazz-tools/react", () => ({
  JazzProvider: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("./contexts/devtools-context", () => ({
  DevtoolsProvider: ({ children }: { children: ReactNode }) => children,
}));

vi.mock("./routes", () => ({
  InspectorRoutes: () => null,
}));
const appId = "shared-app";
const logicalBase = "shared-db";
const externalPhysicalDbName =
  `${logicalBase}::jazz-browser-v1::` +
  "%7B%22version%22%3A1%2C%22appId%22%3A%22shared-app%22%2C%22auth%22%3A%7B%22kind%22%3A%22principal%22%2C%22authMode%22%3A%22external%22%7D%7D";
const localFirstPhysicalDbName =
  `${logicalBase}::jazz-browser-v1::` +
  "%7B%22version%22%3A1%2C%22appId%22%3A%22shared-app%22%2C%22auth%22%3A%7B%22kind%22%3A%22principal%22%2C%22authMode%22%3A%22local-first%22%7D%7D";

const context = (key: string, dbName: string): InspectorRuntimeContext => ({
  key,
  appId,
  dbName,
  schema: {} as WasmSchema,
});

describe("defaultRuntimeContextKey", () => {
  it("chooses the exact host auth scope when reversed contexts share its app and logical base", () => {
    const contexts = [
      context("external-first", externalPhysicalDbName),
      context("host-local-first", localFirstPhysicalDbName),
    ];

    expect(
      defaultRuntimeContextKey(contexts, {
        appId,
        runtimeSources: { inspectorHostPhysicalDbName: localFirstPhysicalDbName },
      }),
    ).toBe("host-local-first");
  });

  it("falls back only when the host context is absent", () => {
    const contexts = [context("external-first", externalPhysicalDbName)];

    expect(
      defaultRuntimeContextKey(contexts, {
        appId,
        runtimeSources: { inspectorHostPhysicalDbName: localFirstPhysicalDbName },
      }),
    ).toBe("external-first");
  });
});

describe("InspectorApp", () => {
  it("retries host discovery until the 15 second deadline", async () => {
    vi.useFakeTimers();
    openSessionMock.mockReset();
    readHostConfigMock.mockReset();
    openSessionMock.mockResolvedValue(null);
    readHostConfigMock.mockReturnValue(null);
    try {
      const view = render(createElement(InspectorApp));
      expect(view.getByText("Connecting…")).toBeDefined();

      await act(() => vi.advanceTimersByTimeAsync(14_999));
      expect(view.queryByText(/no host connection found/)).toBeNull();

      await act(() => vi.advanceTimersByTimeAsync(1));
      expect(view.getByText(/no host connection found/)).toBeDefined();
      expect(openSessionMock.mock.calls.length).toBeGreaterThan(1);
    } finally {
      cleanup();
      vi.useRealTimers();
    }
  });

  it("protocol-closes an attachment acknowledged after unmount", async () => {
    openSessionMock.mockReset();
    readHostConfigMock.mockReset();
    closePortMock.mockReset();
    let resolveAttach: (port: MessagePort) => void = () => {};
    const attach = vi.fn(
      () =>
        new Promise<MessagePort>((resolve) => {
          resolveAttach = resolve;
        }),
    );
    const close = vi.fn();
    const runtimeContext = context("context", localFirstPhysicalDbName);
    openSessionMock.mockResolvedValue({
      contexts: [runtimeContext],
      listContexts: vi.fn(async () => [runtimeContext]),
      attach,
      close,
    });
    readHostConfigMock.mockReturnValue({
      appId,
      runtimeSources: { inspectorHostPhysicalDbName: localFirstPhysicalDbName },
    });
    const view = render(createElement(InspectorApp));
    await vi.waitFor(() => expect(attach).toHaveBeenCalledWith("context"));

    view.unmount();
    const port = {} as MessagePort;
    await act(async () => {
      resolveAttach(port);
      await Promise.resolve();
    });

    expect(closePortMock).toHaveBeenCalledWith(port);
    expect(close).toHaveBeenCalledOnce();
  });
});
