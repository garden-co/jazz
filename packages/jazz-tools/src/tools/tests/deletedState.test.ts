import { beforeEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { cojsonInternals } from "cojson";

import { Account, Group, z } from "../index.js";
import {
  CoValueLoadingState,
  co,
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
} from "../internal.js";
import { loadCoValue } from "../exports.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupAccount, waitFor } from "./utils.js";

cojsonInternals.setCoValueLoadingRetryDelay(50);

describe("deleted loading state", () => {
  beforeEach(() => {
    // Keep these tests snappy and deterministic.
    cojsonInternals.CO_VALUE_LOADING_CONFIG.MAX_RETRIES = 1;
    cojsonInternals.CO_VALUE_LOADING_CONFIG.TIMEOUT = 50;
  });

  it("subscribeToCoValue calls listener with deleted state and stops emitting loaded updates", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const { me, meOnSecondPeer } = await setupAccount();

    const map = TestMap.create({ value: "hello" }, me);

    let loadedCallCount = 0;
    let deletedValue: unknown = null;

    const listener = vi.fn().mockImplementation((value) => {
      if (value.$isLoaded) {
        loadedCallCount++;
      } else if (value.$jazz.loadingState === CoValueLoadingState.DELETED) {
        deletedValue = value;
      }
    });

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestMap),
      map.$jazz.id,
      {
        loadAs: meOnSecondPeer,
      },
      listener,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(loadedCallCount).toBeGreaterThan(0);
    });

    const loadedCallCountBeforeDelete = loadedCallCount;

    map.$jazz.raw.core.deleteCoValue();
    await map.$jazz.raw.core.waitForSync();

    await waitFor(() => {
      expect(deletedValue).not.toBeNull();
    });

    // @ts-expect-error - we know deletedValue is not null here
    expect(deletedValue?.$isLoaded).toBe(false);
    // @ts-expect-error - we know deletedValue is not null here
    expect(deletedValue?.$jazz.loadingState).toBe(CoValueLoadingState.DELETED);

    // Give the system a moment; we should not emit additional loaded updates after deletion.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(loadedCallCount).toBe(loadedCallCountBeforeDelete);
  });

  it("loadCoValue resolves a NotLoaded(DELETED) value for deleted coValues", async () => {
    await setupJazzTestSync();

    const TestMap = co.map({
      value: z.string(),
    });

    const owner = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(owner).makePublic("reader");
    const map = TestMap.create({ value: "hello" }, group);

    // Ensure the value exists on storage/peers before deleting.
    await map.$jazz.raw.core.waitForSync();

    map.$jazz.raw.core.deleteCoValue();
    await map.$jazz.raw.core.waitForSync();

    const viewer = await createJazzTestAccount();

    const loaded = await loadCoValue(
      coValueClassFromCoValueClassOrSchema(TestMap),
      map.$jazz.id,
      {
        loadAs: viewer,
        skipRetry: true,
      },
    );

    expect(loaded.$isLoaded).toBe(false);
    expect(loaded.$jazz.loadingState).toBe(CoValueLoadingState.DELETED);
  });
});
