import React from "react";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetClientRegistryForTest } from "../runtime/client-registry.js";
import { makeFakeAccount, makeFakeClient } from "../react-core/test-utils.js";
import type { DbConfig } from "./create-db.js";

const mocks = vi.hoisted(() => ({
  createJazzClient: vi.fn(),
  createJazzSession: vi.fn(),
}));

vi.mock("./create-jazz-session.js", () => ({ createJazzSession: mocks.createJazzSession }));
vi.mock("react-native", () => ({
  View: "native-view",
  Text: "native-text",
  Pressable: "native-pressable",
}));
vi.mock("./create-jazz-client.js", () => ({
  createJazzClient: mocks.createJazzClient,
}));

import { JazzProvider } from "./provider.js";

beforeEach(() => {
  vi.useFakeTimers();
  mocks.createJazzClient.mockReset();
});

afterEach(async () => {
  cleanup();
  await vi.runAllTimersAsync();
  resetClientRegistryForTest();
  vi.useRealTimers();
});

function makeClient(userId: string) {
  const client = makeFakeClient({ authMode: "local-first", userId, claims: {} });
  client.shutdown = vi.fn().mockResolvedValue(undefined);
  return client;
}

describe("React Native JazzProvider", () => {
  it("reuses its client when a public provider rerender keeps the same native admission capability", async () => {
    const client = makeClient("first");
    mocks.createJazzClient.mockResolvedValue(client);
    const capability = new Uint8Array(32);
    const initialConfig: DbConfig = {
      appId: "native-provider-stable",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount("native-provider-stable"),
      driver: { type: "persistent" },
      nativeRelay: { capability },
    };

    const result = render(
      <JazzProvider config={initialConfig} fallback={null}>
        <div>ready</div>
      </JazzProvider>,
    );
    await act(async () => Promise.resolve());

    const rebuiltConfig: DbConfig = {
      nativeRelay: { capability },
      driver: { type: "persistent" },
      appId: "native-provider-stable",
      serverUrl: initialConfig.serverUrl,
      account: initialConfig.account,
    };
    expect(rebuiltConfig).not.toBe(initialConfig);

    result.rerender(
      <JazzProvider config={rebuiltConfig} fallback={null}>
        <div>ready</div>
      </JazzProvider>,
    );
    await act(async () => Promise.resolve());

    expect(mocks.createJazzClient).toHaveBeenCalledOnce();
    expect(client.shutdown).not.toHaveBeenCalled();
  });

  it("replaces its client when the public provider receives a different native admission capability", async () => {
    const firstClient = makeClient("first");
    const secondClient = makeClient("second");
    mocks.createJazzClient.mockResolvedValueOnce(firstClient).mockResolvedValueOnce(secondClient);
    const initialSource = new Uint8Array(32);
    const replacementSource = new Uint8Array(32);
    const initialConfig: DbConfig = {
      appId: "native-provider-source-swap",
      serverUrl: "https://jazz.example.com",
      account: makeFakeAccount("native-provider-source-swap"),
      driver: { type: "persistent" },
      nativeRelay: { capability: initialSource },
    };

    const result = render(
      <JazzProvider config={initialConfig} fallback={null}>
        <div>ready</div>
      </JazzProvider>,
    );
    await act(async () => Promise.resolve());
    expect(mocks.createJazzClient).toHaveBeenCalledOnce();

    result.rerender(
      <JazzProvider
        config={{
          ...initialConfig,
          nativeRelay: { capability: replacementSource },
        }}
        fallback={null}
      >
        <div>ready</div>
      </JazzProvider>,
    );
    await act(async () => {
      await vi.runAllTimersAsync();
      await Promise.resolve();
    });

    expect(firstClient.shutdown).toHaveBeenCalledOnce();
    expect(mocks.createJazzClient).toHaveBeenCalledTimes(2);
    expect(mocks.createJazzClient.mock.calls[1]?.[0]).toMatchObject({
      nativeRelay: { capability: replacementSource },
    });
  });
});

it("uses native defaults and the native session factory for ergonomic props", async () => {
  mocks.createJazzSession.mockRejectedValue(new Error("native offline"));
  const view = render(
    <JazzProvider
      appId="native-app"
      serverUrl="https://sync.example.test"
      store={{ read: async () => null, update: async () => {} }}
    >
      <span>data</span>
    </JazzProvider>,
  );
  expect(view.container.querySelector("native-view native-text")?.textContent).toBe("Loading…");
  await act(async () => {
    await Promise.resolve();
  });
  expect(mocks.createJazzSession).toHaveBeenCalledWith(
    expect.objectContaining({ initial: "local-first" }),
  );
  expect(view.container.querySelector("native-pressable")?.textContent).toBe("Try again");
  expect(view.container.querySelector("section, p, button")).toBeNull();
});
