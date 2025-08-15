// @vitest-environment happy-dom

import { type CoValue, type ID, co, cojsonInternals, z } from "jazz-tools";
import { createJazzTestAccount, setupJazzTestSync } from "jazz-tools/testing";
import { beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import type { ComputedRef, Ref, ShallowRef } from "vue";
import { useCoState } from "../index.js";
import { waitFor, withJazzTestSetup } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("useCoState", () => {
  it("should return the correct value", async () => {
    const TestMap = co.map({
      content: z.string(),
    });

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
    const TestMap = co.map({
      content: z.string(),
    });

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
    const TestNestedMap = co.map({
      content: z.string(),
    });

    const TestMap = co.map({
      content: z.string(),
      nested: TestNestedMap,
    });

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
    const TestNestedMap = co.map({
      content: z.string(),
    });

    const TestMap = co.map({
      content: z.string(),
      nested: TestNestedMap,
    });

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
    const TestMap = co.map({
      content: z.string(),
    });

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const [result] = withJazzTestSetup(() =>
      useCoState(TestMap, "co_z123" as ID<co.loaded<typeof TestMap>>, {}),
    );

    expect(result.value).toBeUndefined();

    await waitFor(() => {
      expect(result.value).toBeNull();
    });
  });

  it("should return the same type as Schema", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const [result] = withJazzTestSetup(() =>
      useCoState(TestMap, map.id as ID<CoValue>, {
        resolve: true,
      }),
    );
    expectTypeOf(result).toEqualTypeOf<
      Ref<co.loaded<typeof TestMap> | null | undefined>
    >();
  });
});
