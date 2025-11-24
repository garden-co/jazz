// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { CoValueLoadingState, Group, co, z } from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
import { beforeEach, describe, expect, it } from "vitest";
import { useCoStateAndRef } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, useRenderCount, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("useCoStateAndRef", () => {
  it("should return state and ref with the correct values", async () => {
    const TestMap = co.map({
      name: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      name: "test",
      count: 42,
    });

    const { result } = renderHook(
      () => useCoStateAndRef(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    assertLoaded(result.current[0]);
    expect(result.current[0].name).toBe("test");
    expect(result.current[0].count).toBe(42);

    assertLoaded(result.current[1].current);
    expect(result.current[1].$isLoaded).toBe(true);
    expect(result.current[1].current.name).toBe("test");
    expect(result.current[1].current.count).toBe(42);
  });

  it("should only re-render when selected field changes", async () => {
    const TestMap = co.map({
      name: z.string(),
      count: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      name: "test",
      count: 0,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useCoStateAndRef(TestMap, map.$jazz.id, {
            select: (m) => (m.$isLoaded ? m.name : undefined),
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("test");

    // Update field NOT in selector - should not re-render
    act(() => {
      map.$jazz.set("count", 42);
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.count).toBe(42);
    });

    expect(result.current.renderCount).toBe(1);

    // Update field in selector - should re-render
    act(() => {
      map.$jazz.set("name", "updated");
    });

    await waitFor(() => {
      expect(result.current.result[0]).toBe("updated");
    });

    expect(result.current.renderCount).toBe(2);
  });

  it("should allow editing non-selected fields through ref without re-rendering", async () => {
    const TestMap = co.map({
      title: z.string(),
      viewCount: z.number(),
      lastViewed: z.string().optional(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      title: "Document",
      viewCount: 0,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useCoStateAndRef(TestMap, map.$jazz.id, {
            select: (doc) => (doc.$isLoaded ? doc.title : undefined),
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("Document");

    // Edit fields NOT in selector using ref.$jazz - no re-render
    act(() => {
      if (result.current.result[1].current.$isLoaded) {
        result.current.result[1].current.$jazz.set("viewCount", 5);
        result.current.result[1].current.$jazz.set("lastViewed", "2024-01-01");
      }
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.viewCount).toBe(5);
      expect(result.current.result[1].current.lastViewed).toBe("2024-01-01");
    });

    expect(result.current.renderCount).toBe(1);
    expect(map.viewCount).toBe(5);
    expect(map.lastViewed).toBe("2024-01-01");
  });

  it("should handle unavailable state", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "test",
    });

    const viewerAccount = await createJazzTestAccount();

    for (const peer of viewerAccount.$jazz.localNode.syncManager.getServerPeers(
      viewerAccount.$jazz.raw.id,
    )) {
      peer.gracefulShutdown();
    }

    const { result } = renderHook(
      () => useCoStateAndRef(TestMap, map.$jazz.id),
      {
        account: viewerAccount,
      },
    );

    await waitFor(() => {
      expect(result.current[1].current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
      expect(result.current[1].$isLoaded).toBe(false);
    });
  });

  it("should handle unauthorized state", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create(
      {
        value: "secret",
      },
      someoneElse,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () => useCoStateAndRef(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    await waitFor(() => {
      expect(result.current[1].current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
      expect(result.current[1].$isLoaded).toBe(false);
    });
  });

  it("should update when CoValue becomes accessible", async () => {
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

    const { result } = renderHook(
      () => useCoStateAndRef(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    await waitFor(() => {
      expect(result.current[1].current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
      expect(result.current[1].$isLoaded).toBe(false);
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current[1].current.$isLoaded).toBe(true);
      assertLoaded(result.current[0]);
      expect(result.current[0].value).toBe("123");
      assertLoaded(result.current[1].current);
      expect(result.current[1].current.value).toBe("123");
    });
  });

  it("should support custom selector with complex return type", async () => {
    const TestMap = co.map({
      firstName: z.string(),
      lastName: z.string(),
      age: z.number(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      firstName: "John",
      lastName: "Doe",
      age: 30,
    });

    const { result } = renderHook(
      () =>
        useRenderCount(() =>
          useCoStateAndRef(TestMap, map.$jazz.id, {
            select: (user) =>
              user.$isLoaded ? `${user.firstName} ${user.lastName}` : undefined,
          }),
        ),
      {
        account,
      },
    );

    expect(result.current.renderCount).toBe(1);
    expect(result.current.result[0]).toBe("John Doe");

    // Update age (not in selector) - no re-render
    act(() => {
      map.$jazz.set("age", 31);
    });

    await waitFor(() => {
      assertLoaded(result.current.result[1].current);
      expect(result.current.result[1].current.age).toBe(31);
    });

    expect(result.current.renderCount).toBe(1);

    // Update firstName (in selector) - should re-render
    act(() => {
      map.$jazz.set("firstName", "Jane");
    });

    await waitFor(() => {
      expect(result.current.result[0]).toBe("Jane Doe");
    });

    expect(result.current.renderCount).toBe(2);
  });
});
