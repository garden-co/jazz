import React from "react";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetClientRegistryForTest } from "../runtime/client-registry.js";
import { makeFakeClient } from "../react-core/test-utils.js";
import type { DbConfig } from "./create-db.js";

const mocks = vi.hoisted(() => ({
  createJazzClient: vi.fn(),
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
  it("reuses its client when a public provider rerender keeps the same runtime source", async () => {
    const client = makeClient("first");
    mocks.createJazzClient.mockResolvedValue(client);
    const wasmSource = new Uint8Array([0, 97, 115, 109]);
    const initialConfig: DbConfig = {
      appId: "native-provider-stable",
      driver: { type: "memory" },
      runtimeSources: { wasmSource },
    };

    const result = render(
      <JazzProvider config={initialConfig} fallback={null}>
        <div>ready</div>
      </JazzProvider>,
    );
    await act(async () => Promise.resolve());

    const rebuiltConfig: DbConfig = {
      runtimeSources: { wasmSource },
      driver: { type: "memory" },
      appId: "native-provider-stable",
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

  it("replaces its client when the public provider receives a different runtime source", async () => {
    const firstClient = makeClient("first");
    const secondClient = makeClient("second");
    mocks.createJazzClient.mockResolvedValueOnce(firstClient).mockResolvedValueOnce(secondClient);
    const initialSource = new Uint8Array([0, 97, 115, 109]);
    const replacementSource = new Uint8Array([0, 97, 115, 109]);
    const initialConfig: DbConfig = {
      appId: "native-provider-source-swap",
      driver: { type: "memory" },
      runtimeSources: { wasmSource: initialSource },
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
          runtimeSources: { wasmSource: replacementSource },
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
      runtimeSources: { wasmSource: replacementSource },
    });
  });
});
