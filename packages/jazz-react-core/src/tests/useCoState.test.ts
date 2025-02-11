// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import { CoMap, co } from "jazz-tools";
import { beforeEach, describe, expect, it } from "vitest";
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
          nested: {},
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
      () => useCoState(TestMap, (map.id + "123") as any, {}),
      {
        account,
      },
    );

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
    });
  });
});
