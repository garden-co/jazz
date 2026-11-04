import React from "react";
import { act, render, type RenderResult } from "@testing-library/react";
import { renderToStaticMarkup } from "react-dom/server";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { createClientConfigKey } from "../runtime/client-config-key.js";
import type { AccountDbConfig } from "../accounts/context.js";
import { JazzProvider } from "./provider.js";
import { makeFakeAccount, makeFakeClient } from "./test-utils.js";

const registry = vi.hoisted(() => {
  const entries = new Map<string, { promise: Promise<unknown>; holders: Set<object> }>();
  const events: string[] = [];
  const releaseGates = new Map<string, Promise<void>>();

  return {
    entries,
    events,
    releaseGates,
    acquireClient: vi.fn(
      (key: string, create: () => Promise<unknown>, holder: object): Promise<unknown> => {
        events.push(`acquire:${key}`);
        let entry = entries.get(key);
        if (!entry) {
          entry = { promise: create(), holders: new Set() };
          entries.set(key, entry);
        }
        entry.holders.add(holder);
        return entry.promise;
      },
    ),
    releaseClient: vi.fn(async (key: string, holder: object): Promise<void> => {
      events.push(`release:${key}`);
      await releaseGates.get(key);
      const entry = entries.get(key);
      if (!entry) return;
      entry.holders.delete(holder);
      if (entry.holders.size === 0) entries.delete(key);
    }),
  };
});

vi.mock("../runtime/client-registry.js", () => ({
  acquireClient: registry.acquireClient,
  releaseClient: registry.releaseClient,
}));

beforeEach(() => {
  registry.entries.clear();
  registry.releaseGates.clear();
  registry.events.length = 0;
  registry.acquireClient.mockClear();
  registry.releaseClient.mockClear();
});

describe("JazzProvider client acquisition lifecycle", () => {
  it("renders its fallback on the server without acquiring or retaining a client", () => {
    const openJazzClient = vi.fn(async () =>
      makeFakeClient({ authMode: "local-first", userId: "server", claims: {} }),
    );

    const html = renderToStaticMarkup(
      <JazzProvider
        config={{
          appId: "app-1",
          serverUrl: "https://jazz.example.com",
          account: makeFakeAccount(),
        }}
        openJazzClient={openJazzClient}
        fallback={<p id="loading">loading</p>}
      >
        <p>ready</p>
      </JazzProvider>,
    );

    expect(html).toBe('<p id="loading">loading</p>');
    expect(registry.acquireClient).not.toHaveBeenCalled();
    expect(openJazzClient).not.toHaveBeenCalled();
    expect(registry.entries.size).toBe(0);
  });

  it("acquires one lease after mount and releases each config on replacement and unmount", async () => {
    const initialConfig: AccountDbConfig = {
      appId: "app-1",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount(),
    };
    const replacementConfig: AccountDbConfig = {
      appId: "app-1",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount(),
    };
    const openJazzClient = vi.fn(async () =>
      makeFakeClient({ authMode: "local-first", userId: "browser", claims: {} }),
    );
    const initialKey = createClientConfigKey("react", initialConfig, [openJazzClient]);
    const replacementKey = createClientConfigKey("react", replacementConfig, [openJazzClient]);

    let result!: RenderResult;
    await act(async () => {
      result = render(
        <JazzProvider
          config={initialConfig}
          openJazzClient={openJazzClient}
          fallback={<p>loading</p>}
        >
          <p>ready</p>
        </JazzProvider>,
      );
      await Promise.resolve();
    });

    expect(registry.events).toEqual([`acquire:${initialKey}`]);
    expect(registry.acquireClient).toHaveBeenCalledTimes(1);
    expect(registry.entries.get(initialKey)?.holders.size).toBe(1);
    const holder = registry.acquireClient.mock.calls[0]?.[2];

    await act(async () => {
      result.rerender(
        <JazzProvider
          config={replacementConfig}
          openJazzClient={openJazzClient}
          fallback={<p>loading</p>}
        >
          <p>ready</p>
        </JazzProvider>,
      );
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(registry.events).toEqual([
      `acquire:${initialKey}`,
      `release:${initialKey}`,
      `acquire:${replacementKey}`,
    ]);
    expect(registry.entries.has(initialKey)).toBe(false);
    expect(registry.entries.get(replacementKey)?.holders).toEqual(new Set([holder]));

    await act(async () => {
      result.unmount();
      await Promise.resolve();
    });

    expect(registry.events).toEqual([
      `acquire:${initialKey}`,
      `release:${initialKey}`,
      `acquire:${replacementKey}`,
      `release:${replacementKey}`,
    ]);
    expect(registry.entries.size).toBe(0);
  });

  it("does not acquire a replacement cancelled behind an in-flight release", async () => {
    const initialConfig: AccountDbConfig = {
      appId: "app-1",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount(),
    };
    const replacementConfig: AccountDbConfig = {
      appId: "app-1",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount(),
    };
    let resolveRelease!: () => void;
    const releaseGate = new Promise<void>((resolve) => {
      resolveRelease = resolve;
    });
    const openJazzClient = vi.fn(async () =>
      makeFakeClient({ authMode: "local-first", userId: "browser", claims: {} }),
    );
    const initialKey = createClientConfigKey("react", initialConfig, [openJazzClient]);

    let result!: RenderResult;
    await act(async () => {
      result = render(
        <JazzProvider
          config={initialConfig}
          openJazzClient={openJazzClient}
          fallback={<p>loading</p>}
        >
          <p>ready</p>
        </JazzProvider>,
      );
      await Promise.resolve();
    });
    registry.releaseGates.set(initialKey, releaseGate);

    await act(async () => {
      result.rerender(
        <JazzProvider
          config={replacementConfig}
          openJazzClient={openJazzClient}
          fallback={<p>loading</p>}
        >
          <p>ready</p>
        </JazzProvider>,
      );
      await Promise.resolve();
    });
    expect(registry.acquireClient).toHaveBeenCalledTimes(1);

    await act(async () => {
      result.unmount();
      resolveRelease();
      await releaseGate;
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(registry.acquireClient).toHaveBeenCalledTimes(1);
    expect(registry.entries.size).toBe(0);
  });
});
