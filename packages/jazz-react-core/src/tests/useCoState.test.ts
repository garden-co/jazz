// @vitest-environment happy-dom

import { CoMap, co } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { useCoState } from "../index.js";
import { createJazzTestAccount } from "../testing.js";
import { act, renderHook } from "./testUtils.js";

describe("useCoState", () => {
  it("should return the correct value", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: "123",
      },
      { owner: account },
    );

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
  });

  it("should update the value when the coValue changes", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: "123",
      },
      { owner: account },
    );

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

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: "123",
        nested: TestNestedMap.create(
          {
            value: "456",
          },
          { owner: account },
        ),
      },
      { owner: account },
    );

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

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        value: "123",
        nested: TestNestedMap.create(
          {
            value: "456",
          },
          { owner: account },
        ),
      },
      { owner: account },
    );

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
    expect(result.current?.nested?.value).toBe("456");
  });
});
