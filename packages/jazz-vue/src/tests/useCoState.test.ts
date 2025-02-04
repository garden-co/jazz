// @vitest-environment happy-dom

import { CoMap, co } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { describe, expect, it } from "vitest";
import { useCoState } from "../index.js";
import { withJazzTestSetup } from "./testUtils.js";

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
          resolve: {
            nested: true,
          },
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
});
