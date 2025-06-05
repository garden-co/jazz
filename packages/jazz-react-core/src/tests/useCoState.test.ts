// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import {
  Account,
  CoRichText,
  CoValue,
  Group,
  ID,
  Loaded,
  co,
  z,
} from "jazz-tools";
import { assert, beforeEach, describe, expect, expectTypeOf, it } from "vitest";
import { useCoState } from "../index.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { act, renderHook, waitFor } from "./testUtils.js";

beforeEach(async () => {
  await setupJazzTestSync();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

cojsonInternals.setCoValueLoadingRetryDelay(300);

describe("useCoState", () => {
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

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
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

    const { result } = renderHook(() => useCoState(TestMap, map.id, {}), {
      account,
    });

    expect(result.current?.value).toBe("123");
    expect(result.current?.nested?.value).toBe("456");
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

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current).toBeNull();
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

    const { result } = renderHook(() => useCoState(TestMap, map.id), {
      account,
    });

    expect(result.current).toBeUndefined();

    await waitFor(() => {
      expect(result.current?.value).toBe("123");
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

  it("should return a null value when the coValue becomes inaccessible", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const { result } = renderHook(() => useCoState(TestMap, undefined));

    expect(result.current).toBeNull();
  });

  it("should update when an inner coValue is updated", async () => {
    const TestMap = co.map({
      value: z.string(),
      get nested() {
        return TestMap.optional();
      },
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
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result } = renderHook(() =>
      useCoState(TestMap, map.id as ID<CoValue>),
    );
    expectTypeOf(result).toEqualTypeOf<{
      current: Loaded<typeof TestMap> | null | undefined;
    }>();
  });

  it("should set the value to undefined when the id is set to undefined", () => {
    const TestMap = co.map({
      value: z.string(),
    });

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

  it("should only render once when loading a list of values", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

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

    expect(renderCount).toBe(1);
  });

  it("should manage correctly the group.members[number].account.profile?.name autoload", async () => {
    const Dog = co.map({
      name: z.string(),
    });

    const john = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: {
        name: "John Doe",
      },
    });

    const jane = await createJazzTestAccount({
      creationProps: {
        name: "Jane Doe",
      },
    });

    const janeOnJohn = await Account.load(jane.id, {
      loadAs: john,
    });

    assert(janeOnJohn);

    const group = Group.create(john);
    group.addMember(janeOnJohn, "reader");

    const dog = Dog.create(
      {
        name: "Rex",
      },
      group,
    );

    const { result } = renderHook(
      () => useCoState(Dog, dog.id)?._owner.castAs(Group).members,
      {
        account: john,
      },
    );

    await waitFor(() => {
      expect(result.current?.[0]?.account?.profile?.name).toBe("John Doe");
      expect(result.current?.[1]?.account?.profile?.name).toBe("Jane Doe");
    });
  });

  it("should immediately load deeploaded data when available locally", async () => {
    const Message = co.map({
      content: CoRichText,
    });
    const Thread = co.map({
      messages: co.list(Message),
    });

    const thread = Thread.create({
      messages: Thread.def.shape.messages.create([
        Message.create({
          content: CoRichText.create("Hello man!"),
        }),
        Message.create({
          content: CoRichText.create("The temperature is high today"),
        }),
        Message.create({
          content: CoRichText.create("Shall we go to the beach?"),
        }),
      ]),
    });

    const renderings: boolean[] = [];

    renderHook(() => {
      const data = useCoState(Thread, thread.id, {
        resolve: {
          messages: {
            $each: {
              content: true,
            },
          },
        },
      });

      renderings.push(Boolean(data));
    });

    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(renderings).toEqual([true]);
  });
});
