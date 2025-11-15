// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { CoValueLoadingState, Group, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useCoValueRef } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, useRenderCount, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("useCoValueRef", () => {
  it("should return a ref with the correct value", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");
  });

  it("should return a ref with 'unavailable' value on invalid id", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, "test"), {
      account,
    });

    expect(coValueRef.current.$jazz.loadingState).toBe(
      CoValueLoadingState.LOADING,
    );

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });
  });

  it("should update the ref when the coValue changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");

    act(() => {
      map.$jazz.set("value", "456");
    });

    await waitFor(() => {
      assertLoaded(coValueRef.current);
      expect(coValueRef.current.value).toBe("456");
    });
  });

  it("should load nested values if requested", async () => {
    const TestNestedMap = co.map({
      value: z.string(),
    });

    const TestMap = co.map({
      value: z.string(),
      nested: TestNestedMap,
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
      nested: TestNestedMap.create({
        value: "456",
      }),
    });

    const {
      result: { current: coValueRef },
    } = renderHook(
      () =>
        useCoValueRef(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");
    expect(coValueRef.current.nested.value).toBe("456");
  });

  it("should return a ref with 'unavailable' value if the coValue is not found", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const viewerAccount = await createJazzTestAccount();

    for (const peer of viewerAccount.$jazz.localNode.syncManager.getServerPeers(
      viewerAccount.$jazz.raw.id,
    )) {
      peer.gracefulShutdown();
    }

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account: viewerAccount,
    });

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });
  });

  it("should return a ref with 'unauthorized' value if the coValue is not accessible", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create(
      {
        value: "123",
      },
      someoneElse,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  it("should return a ref with 'loaded' value if the coValue is shared with everyone", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(someoneElse);
    group.addMember("everyone", "reader");

    const map = TestMap.create(
      {
        value: "123",
      },
      group,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      assertLoaded(coValueRef.current);
      expect(coValueRef.current.value).toBe("123");
    });
  });

  it("should update ref when the coValue becomes accessible", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(someoneElse);

    const map = TestMap.create(
      {
        value: "123",
      },
      group,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).not.toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");
  });

  it("should return a ref with 'unauthorized' value when the coValue becomes inaccessible", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const group = Group.create(someoneElse);

    const map = TestMap.create(
      {
        value: "123",
      },
      group,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    await account.$jazz.waitForAllCoValuesSync();

    group.addMember(account, "reader");

    const {
      result: { current: coValueRef },
    } = renderHook(() => useCoValueRef(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(coValueRef.current).not.toBeUndefined();
    });

    group.removeMember(account);

    await waitFor(() => {
      expect(coValueRef.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  it("should not cause re-renders when the coValue changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const {
      result: {
        current: { renderCount, result: coValueRef },
      },
    } = renderHook(
      () => useRenderCount(() => useCoValueRef(TestMap, map.$jazz.id)),
      {
        account,
      },
    );

    expect(renderCount).toBe(1);

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");

    act(() => {
      map.$jazz.set("value", "456");
    });

    await waitFor(() => {
      assertLoaded(coValueRef.current);
      expect(coValueRef.current.value).toBe("456");
    });

    expect(renderCount).toBe(1);
  });

  it("should allow editing the coValue through the ref without causing re-renders", async () => {
    const TestMap = co.map({
      value: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
      count: 0,
    });

    const {
      result: {
        current: { renderCount, result: coValueRef },
      },
    } = renderHook(
      () => useRenderCount(() => useCoValueRef(TestMap, map.$jazz.id)),
      {
        account,
      },
    );

    assertLoaded(coValueRef.current);
    expect(coValueRef.current.value).toBe("123");
    expect(coValueRef.current.count).toBe(0);
    expect(renderCount).toBe(1);

    act(() => {
      const current = coValueRef.current;
      if (current.$isLoaded) {
        current.$jazz.set("value", "updated");
        current.$jazz.set("count", 42);
      }
    });

    await waitFor(() => {
      assertLoaded(coValueRef.current);
      expect(coValueRef.current.value).toBe("updated");
      expect(coValueRef.current.count).toBe(42);
    });

    expect(renderCount).toBe(1);

    expect(map.value).toBe("updated");
    expect(map.count).toBe(42);
  });
});
