// @vitest-environment happy-dom
// @ts-nocheck

import { cojsonInternals } from "cojson";
import { CoValue, Group, ID, SchemaV2, co } from "jazz-tools";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { useCoState2 } from "../useCoState2.js";
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

describe("useCoState2", () => {
  it("should return the correct value", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => useCoState2(TestMap, map.$id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
  });

  it("should update the value when the coValue changes", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => useCoState2(TestMap, map.$id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");

    act(() => {
      map.$set("value", "456");
    });

    expect(result.current?.value).toBe("456");
  });

  it("should load nested values if requested", async () => {
    class TestNestedMap extends SchemaV2.CoMap {
      value = co.string;
    }

    class TestMap extends SchemaV2.CoMap {
      value = co.string;
      nested = co.ref(TestNestedMap);
    }

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
        useCoState2(TestMap, map.$id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.current?.value).toBe("123");
    expect(result.current?.nested.value).toBe("456");
  });

  it("should load nested values when $requested", async () => {
    class TestNestedMap extends SchemaV2.CoMap {
      value = co.string;
    }

    class TestMap extends SchemaV2.CoMap {
      value = co.string;
      nested = co.ref(TestNestedMap);
    }

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
      nested: TestNestedMap.create({
        value: "456",
      }),
    });

    const { result } = renderHook(() => useCoState2(TestMap, map.$id, {}), {
      account,
    });

    result.current?.$request({
      resolve: {
        nested: true,
      },
    });

    expect(result.current?.value).toBe("123");

    await waitFor(() => {
      expect(result.current?.nested.value).toBe("456");
    });
  });

  it.skip("should return null if the coValue is not found", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () => useCoState2(TestMap, (map.id + "123") as any),
      {
        account,
      },
    );

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it.skip("should return null if the coValue is not accessible", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

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

    const { result } = renderHook(() => useCoState2(TestMap, map.$id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it("should not return null if the coValue is shared with everyone", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

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

    const { result } = renderHook(() => useCoState2(TestMap, map.$id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });
  });

  it.skip("should return a value when the coValue becomes accessible", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

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

    const { result } = renderHook(() => useCoState2(TestMap, map.$id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current).not.toBeNull();
      expect(result.current?.value).toBe("123");
    });
  });

  it.skip("should update when an inner coValue is updated", async () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
      nested = co.optional.ref(TestMap);
    }

    const someoneElse = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(someoneElse);
    everyone.addMember("everyone", "reader");
    const group = Group.create(someoneElse);

    const map = TestMap.create(
      {
        value: "123",
        nested: TestMap.create(
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
        useCoState2(TestMap, map.$id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).not.toBeUndefined();
    });

    expect(result.current?.nested).toBeUndefined();
    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current?.nested?.value).toBe("456");
    });
  });

  it.skip("should return the same type as Schema", () => {
    class TestMap extends SchemaV2.CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() =>
      useCoState2(TestMap, map.$id as ID<CoValue>),
    );
    expectTypeOf(result).toEqualTypeOf<{
      current: TestMap | null | undefined;
    }>();
  });
});
