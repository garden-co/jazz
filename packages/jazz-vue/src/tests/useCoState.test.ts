// @vitest-environment happy-dom

import { CoMap, CoValue, ID, co, cojsonInternals } from "jazz-tools";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { Ref } from "vue";
import { useCoState } from "../index.js";
import { waitFor, withJazzTestSetup } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

beforeEach(() => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.MAX_RETRIES = 1;
  cojsonInternals.CO_VALUE_LOADING_CONFIG.TIMEOUT = 1;
});

describe("useCoState", () => {
  it("should return the correct value", async () => {
    class TestMap extends CoMap {
      content = co.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        content: "123",
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.value?.content).toBe("123");
  });

  it("should update the value when the coValue changes", async () => {
    class TestMap extends CoMap {
      content = co.string;
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        content: "123",
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.value?.content).toBe("123");

    map.content = "456";

    expect(result.value?.content).toBe("456");
  });

  it("should load nested values if requested", async () => {
    class TestNestedMap extends CoMap {
      content = co.string;
    }

    class TestMap extends CoMap {
      content = co.string;
      nested = co.ref(TestNestedMap);
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        content: "123",
        nested: TestNestedMap.create(
          {
            content: "456",
          },
          { owner: account },
        ),
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(
      () =>
        useCoState(TestMap, map.id, {
          nested: {},
        }),
      {
        account,
      },
    );

    expect(result.value?.content).toBe("123");
    expect(result.value?.nested?.content).toBe("456");
  });

  it("should load nested values on access even if not requested", async () => {
    class TestNestedMap extends CoMap {
      content = co.string;
    }

    class TestMap extends CoMap {
      content = co.string;
      nested = co.ref(TestNestedMap);
    }

    const account = await createJazzTestAccount();

    const map = TestMap.create(
      {
        content: "123",
        nested: TestNestedMap.create(
          {
            content: "456",
          },
          { owner: account },
        ),
      },
      { owner: account },
    );

    const [result] = withJazzTestSetup(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.value?.content).toBe("123");
    expect(result.value?.nested?.content).toBe("456");
  });

  it("should return null if the coValue is not found", async () => {
    class TestMap extends CoMap {
      content = co.string;
    }

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const [result] = withJazzTestSetup(() =>
      useCoState(TestMap, "co_z123" as ID<TestMap>, {}),
    );

    expect(result.value).toBeUndefined();

    await waitFor(() => {
      expect(result.value).toBeNull();
    });
  });

  it("should return the same type as Schema", () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    const map = TestMap.create({
      value: "123",
    });

    const [result] = withJazzTestSetup(() =>
      useCoState(TestMap, map.id as ID<CoValue>, []),
    );
    expectTypeOf(result).toEqualTypeOf<
      Ref<TestMap | null | undefined, TestMap | null | undefined>
    >();
  });
});
