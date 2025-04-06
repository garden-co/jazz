// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { CoList, CoMap, CoValue, Group, ID, co } from "jazz-tools";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { useCoState } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
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

describe("useCoState", () => {
  it("should return the correct value", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
  });

  it("should update the value when the coValue changes", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");

    act(() => {
      map.value = "456";
    });

    expect(result.current?.value).toBe("456");
  });

  it("should load nested values if requested", async () => {
    class TestNestedMap extends CoMap {
      value = co.string;
    }

    class TestMap extends CoMap {
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
        useCoState(TestMap, map.id, {
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

  it("should load nested values on access even if not requested", async () => {
    class TestNestedMap extends CoMap {
      value = co.string;
    }

    class TestMap extends CoMap {
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

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
    expect(result.current?.nested?.value).toBe("456");
  });

  it("should return null if the coValue is not found", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(
      () => useCoState(TestMap, (map.id + "123") as any),
      {
        account,
      },
    );

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it("should return null if the coValue is not accessible", async () => {
    class TestMap extends CoMap {
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

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it("should not return null if the coValue is shared with everyone", async () => {
    class TestMap extends CoMap {
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

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });
  });

  it("should return a value when the coValue becomes accessible", async () => {
    class TestMap extends CoMap {
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

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    expect(result.current?.value).toBe("123");
  });

  it("should return a null value when the coValue becomes inaccessible", async () => {
    class TestMap extends CoMap {
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

    await account.waitForAllCoValuesSync();

    group.addMember(account, "reader");

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).not.toBeUndefined();
    });

    group.removeMember(account);

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });

  it("should update when an inner coValue is updated", async () => {
    class TestMap extends CoMap {
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
        useCoState(TestMap, map.id, {
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

  it("should return the same type as Schema", () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() =>
      useCoState(TestMap, map.id as ID<CoValue>),
    );
    expectTypeOf(result).toEqualTypeOf<{
      current: TestMap | null | undefined;
    }>();
  });

  it("should set the value to undefined when the id is set to undefined", () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const { result, rerender } = renderHook(
      (props) => useCoState(TestMap, props.id),
      {
        initialProps: { id: map.id } as { id: ID<CoValue> | undefined },
      },
    );

    expect(result.current?.value).toBe("123");

    rerender({ id: undefined });

    expect(result.current?.value).toBeUndefined();
  });

  it("should only render twice when loading a list of values", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    class TestList extends CoList.Of(co.ref(TestMap)) {}

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const list = TestList.create([
      TestMap.create({ value: "1" }),
      TestMap.create({ value: "2" }),
      TestMap.create({ value: "3" }),
      TestMap.create({ value: "4" }),
      TestMap.create({ value: "5" }),
    ]);

    let renderCount = 0;

    renderHook(
      () => {
        renderCount++;
        useCoState(TestList, list.id, { resolve: { $each: true } });
      },
      {
        account,
      },
    );

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(renderCount).toBe(2);
  });
});
