// @vitest-environment happy-dom

import {
  type CoValue,
  type ID,
  type MaybeLoaded,
  CoValueLoadingState,
  co,
  cojsonInternals,
  z,
} from "jazz-tools";
import {
  assertLoaded,
  createJazzTestAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
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

    const [result] = withJazzTestSetup(
      () => useCoState(TestMap, map.$jazz.id, {}),
      {
        account,
      },
    );

    assertLoaded(result.value);
    expect(result.value.content).toBe("123");
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

    const [result] = withJazzTestSetup(
      () => useCoState(TestMap, map.$jazz.id, {}),
      {
        account,
      },
    );

    assertLoaded(result.value);
    expect(result.value.content).toBe("123");

    map.$jazz.set("content", "456");

    expect(result.value.content).toBe("456");
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
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(result.value);
    expect(result.value.content).toBe("123");
    expect(result.value.nested.content).toBe("456");
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

    const [result] = withJazzTestSetup(
      () => useCoState(TestMap, map.$jazz.id, {}),
      {
        account,
      },
    );

    assertLoaded(result.value);
    expect(result.value.content).toBe("123");
    assertLoaded(result.value.nested);
    expect(result.value.nested.content).toBe("456");
  });

  it("should return a 'unavailable' value if the coValue is not found", async () => {
    const TestMap = co.map({
      content: z.string(),
    });

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const [result] = withJazzTestSetup(() =>
      useCoState(TestMap, "co_z123" as ID<co.loaded<typeof TestMap>>, {}),
    );

    expect(result.value.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);

    await waitFor(() => {
      expect(result.value.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
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
      useCoState(TestMap, map.$jazz.id as ID<CoValue>, {
        resolve: true,
      }),
    );
    expectTypeOf(result).toEqualTypeOf<
      Ref<MaybeLoaded<co.loaded<typeof TestMap>>>
    >();
  });
});
