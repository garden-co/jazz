// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { Group, SchemaV2 } from "jazz-tools";
import { assert, beforeEach, describe, expect, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { useCoStateWithZod } from "../useCoStateWithZod.js";
import { act, renderHook, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

beforeEach(() => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.MAX_RETRIES = 1;
  cojsonInternals.CO_VALUE_LOADING_CONFIG.TIMEOUT = 1;
});

const { co, z } = SchemaV2;

describe("useCoStateWithZod", () => {
  it("should return the correct value", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(
      () => useCoStateWithZod(TestMap, map.$jazz.id, {}),
      {
        account,
      },
    );

    assert(result.current.$jazzState === "loaded");

    expect(result.current.value).toBe("123");
  });

  it("should update the value when the coValue changes", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(
      () => useCoStateWithZod(TestMap, map.$jazz.id, {}),
      {
        account,
      },
    );
    assert(result.current.$jazzState === "loaded");

    expect(result.current.value).toBe("123");

    act(() => {
      map.$jazz.set("value", "456");
    });

    expect(result.current.value).toBe("456");
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

    const { result } = renderHook(
      () =>
        useCoStateWithZod(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    assert(result.current.$jazzState === "loaded");

    expect(result.current.value).toBe("123");
    expect(result.current.nested.value).toBe("456");
  });

  it("should return null if the coValue is not found", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () => useCoStateWithZod(TestMap, (map.$jazz.id + "123") as any),
      {
        account,
      },
    );

    expect(result.current.$jazzState).toBe("unloaded");

    await waitFor(() => {
      expect(result.current.$jazzState).toBe("unavailable");
    });
  });

  it("should return null if the coValue is not accessible", async () => {
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

    const { result } = renderHook(
      () => useCoStateWithZod(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    expect(result.current.$jazzState).toBe("unloaded");

    await waitFor(() => {
      expect(result.current.$jazzState).toBe("unauthorized");
    });
  });

  it("should not return null if the coValue is shared with everyone", async () => {
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

    const { result } = renderHook(
      () => useCoStateWithZod(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    expect(result.current.$jazzState).toBe("unloaded");

    await waitFor(() => {
      assert(result.current.$jazzState === "loaded");
      expect(result.current.value).toBe("123");
    });
  });

  it("should return a value when the coValue becomes accessible", async () => {
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
      () => useCoStateWithZod(TestMap, map.$jazz.id),
      {
        account,
      },
    );

    expect(result.current.$jazzState).toBe("unloaded");

    await waitFor(() => {
      assert(result.current.$jazzState === "unauthorized");
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current.$jazzState).toBe("loaded");
      assert(result.current.$jazzState === "loaded");
      expect(result.current.value).toBe("123");
    });
  });

  it("should update when an nested coValue becomes available", async () => {
    const TestMap = co.map({
      value: z.string(),
      nested: co.map({
        value: z.string(),
      }),
    });

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(someoneElse);
    everyone.addMember("everyone", "reader");
    const group = Group.create(someoneElse);

    const map = TestMap.create(
      {
        value: "123",
        nested: TestMap.shape.nested.create(
          {
            value: "456",
          },
          group,
        ),
      },
      everyone,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () =>
        useCoStateWithZod(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.current.$jazzState).toBe("unloaded");

    await waitFor(() => {
      expect(result.current.$jazzState).toBe("unauthorized");
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current.$jazzState).toBe("loaded");
      assert(result.current.$jazzState === "loaded");
      expect(result.current.nested.value).toBe("456");
    });
  });
});
