import { render, waitFor } from "@testing-library/svelte";
import { Account, CoMap, co, type CoValue, type CoValueClass, type DepthsIn } from "jazz-tools";
import { describe, expect, it } from "vitest";
import { createJazzTestAccount, createJazzTestContext } from "../testing.js";
import UseCoState from "./components/useCoState.svelte";

function setup<T extends CoValue>(options: {
  account: Account;
  map: T;
  depth?: DepthsIn<T>;
}) {
  const result = { current: undefined } as { current: T | undefined };

    render(UseCoState, {
      context: createJazzTestContext({ account: options.account }),
      props: {
        Schema: options.map.constructor as CoValueClass<T>,
        id: options.map.id,
        depth: options.depth ?? [],
        setResult: (value: T | undefined) => {
          result.current = value
        },
      },
    });

  return result;
}

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

    const result = setup({
      account,
      map,
    });

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });
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

    const result = setup({
      account,
      map,
    });

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });

    map.value = "456";

    await waitFor(() => {
      expect(result.current?.value).toBe("456");
    });
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
   
    const result = setup({
      account,
      map,
      depth: {
        nested: {},
      },
    });

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });

    expect(result.current?.nested?.value).toBe("456");
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

    const result = setup({
      account,
      map,
      depth: {
        nested: {},
      },
    });

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
    });
    
    expect(result.current?.nested?.value).toBe("456");
  });
});