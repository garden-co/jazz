import React from "react";
import { act, cleanup, render } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resetClientRegistryForTest } from "../runtime/client-registry.js";

const mocks = vi.hoisted(() => ({
  openJazzClient: vi.fn(),
  createJazzClient: vi.fn(),
}));

vi.mock("./create-jazz-session.js", () => ({ openNativeSessionCore: mocks.createJazzClient }));

vi.mock("./create-jazz-client.js", () => ({
  openJazzClient: mocks.openJazzClient,
}));

import { JazzProvider } from "./provider.js";

beforeEach(() => {
  vi.useFakeTimers();
  mocks.openJazzClient.mockReset();
});

afterEach(async () => {
  cleanup();
  await vi.runAllTimersAsync();
  resetClientRegistryForTest();
  vi.useRealTimers();
});

it("uses native defaults and the native session factory", async () => {
  mocks.createJazzClient.mockRejectedValue(new Error("native offline"));
  const view = render(
    <JazzProvider
      config={{
        scope: "native-app",
        serverUrl: "https://sync.example.test",
        store: { read: async () => null, update: async () => {} },
      }}
    >
      <span>data</span>
    </JazzProvider>,
  );
  expect(view.container.querySelector("native-view native-text")?.textContent).toBe("Loading…");
  await act(async () => {
    await Promise.resolve();
  });
  expect(mocks.createJazzClient).toHaveBeenCalledOnce();
  expect(mocks.createJazzClient.mock.calls[0][0].initial).toBeUndefined();
  expect(view.container.querySelector("native-pressable")?.textContent).toBe("Try again");
  expect(view.container.querySelector("section, p, button")).toBeNull();
});
