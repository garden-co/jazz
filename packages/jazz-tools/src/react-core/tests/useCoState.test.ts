// @vitest-environment happy-dom

import { cojsonInternals } from "cojson";
import {
  Account,
  CoRichText,
  CoValue,
  CoValueLoadingState,
  Group,
  ID,
  Loaded,
  MaybeLoaded,
  co,
  z,
} from "jazz-tools";
import { assertLoaded } from "jazz-tools/testing";
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id, {}), {
      account,
    });

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
  });

  it("should return an 'unavailable' value on invalid id", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const { result } = renderHook(() => useCoState(TestMap, "test", {}), {
      account,
    });

    expect(result.current.$jazz.loadingState).toBe(
      CoValueLoadingState.UNLOADED,
    );

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id, {}), {
      account,
    });

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");

    act(() => {
      map.$jazz.set("value", "456");
    });

    expect(result.current.value).toBe("456");
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
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
    expect(result.current.nested.value).toBe("456");
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id, {}), {
      account,
    });

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
    assertLoaded(result.current.nested);
    expect(result.current.nested.value).toBe("456");
  });

  it("should return an 'unavailable' value if the coValue is not found", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const viewerAccount = await createJazzTestAccount();

    for (const peer of viewerAccount.$jazz.localNode.syncManager.getServerPeers(
      viewerAccount.$jazz.raw.id,
    )) {
      peer.gracefulShutdown();
    }

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id), {
      account: viewerAccount,
    });

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAVAILABLE,
      );
    });
  });

  it("should return an 'unauthorized' value if the coValue is not accessible", async () => {
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  it("should return a 'loaded' value if the coValue is shared with everyone", async () => {
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      assertLoaded(result.current);
      expect(result.current.value).toBe("123");
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

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).not.toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");
  });

  it("should return an 'unauthorized' value when the coValue becomes inaccessible", async () => {
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

    await account.$jazz.waitForAllCoValuesSync();

    group.addMember(account, "reader");

    const { result } = renderHook(() => useCoState(TestMap, map.$jazz.id), {
      account,
    });

    await waitFor(() => {
      expect(result.current).not.toBeUndefined();
    });

    group.removeMember(account);

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });
  });

  it("should return a 'unavailable' value when no id is provided", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const { result } = renderHook(() => useCoState(TestMap, undefined));

    expect(result.current.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAVAILABLE,
    );
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
        useCoState(TestMap, map.$jazz.id, {
          resolve: {
            nested: true,
          },
        }),
      {
        account,
      },
    );

    await waitFor(() => {
      expect(result.current.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      assertLoaded(result.current);
      assert(result.current.nested);
      expect(result.current.nested.value).toBe("456");
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
      useCoState(TestMap, map.$jazz.id as ID<CoValue>),
    );
    expectTypeOf(result).toEqualTypeOf<{
      current: MaybeLoaded<Loaded<typeof TestMap>>;
    }>();
  });

  it("should set the value to 'unavailable' when the id is set to undefined", () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const map = TestMap.create({
      value: "123",
    });

    const { result, rerender } = renderHook(
      (props) => useCoState(TestMap, props.id),
      {
        initialProps: { id: map.$jazz.id } as { id: ID<CoValue> | undefined },
      },
    );

    assertLoaded(result.current);
    expect(result.current.value).toBe("123");

    rerender({ id: undefined });

    expect(result.current.$jazz.loadingState).toBe(
      CoValueLoadingState.UNAVAILABLE,
    );
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
        useCoState(TestList, list.$jazz.id, { resolve: { $each: true } });
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

    const janeOnJohn = await Account.load(jane.$jazz.id, {
      loadAs: john,
    });

    assertLoaded(janeOnJohn);

    const group = Group.create(john);
    group.addMember(janeOnJohn, "reader");

    const dog = Dog.create(
      {
        name: "Rex",
      },
      group,
    );

    const { result } = renderHook(
      () => {
        const loadedDog = useCoState(Dog, dog.$jazz.id);
        if (!loadedDog.$isLoaded) {
          return undefined;
        }
        return loadedDog.$jazz.owner.members;
      },
      {
        account: john,
      },
    );

    await waitFor(() => {
      const johnsAccount = result.current?.[0]?.account;
      const janesAccount = result.current?.[1]?.account;
      assert(johnsAccount);
      assert(janesAccount);
      assertLoaded(johnsAccount.profile);
      assertLoaded(janesAccount.profile);
      expect(johnsAccount.profile.name).toBe("John Doe");
      expect(janesAccount.profile.name).toBe("Jane Doe");
    });
  });

  it("should immediately load deeploaded data when available locally", async () => {
    const Message = co.map({
      content: CoRichText,
    });
    const Messages = co.list(Message);
    const Thread = co.map({
      messages: Messages,
    });

    const thread = Thread.create({
      messages: Messages.create([
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
      const data = useCoState(Thread, thread.$jazz.id, {
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

  it("should work with branches - create branch, edit and merge", async () => {
    const Person = co.map({
      name: z.string(),
      age: z.number(),
      email: z.string(),
    });

    const group = Group.create();
    group.addMember("everyone", "writer");

    const originalPerson = Person.create(
      {
        name: "John Doe",
        age: 30,
        email: "john@example.com",
      },
      group,
    );

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Use useCoState with the branch
    const { result } = renderHook(
      () => {
        const branch = useCoState(Person, originalPerson.$jazz.id, {
          unstable_branch: { name: "feature-branch" },
        });

        const main = useCoState(Person, originalPerson.$jazz.id);

        return { branch, main };
      },
      {
        account,
      },
    );

    await waitFor(() => {
      expect(result.current).not.toBeNull();
    });

    const branchPerson = result.current.branch;

    assertLoaded(branchPerson);

    act(() => {
      branchPerson.$jazz.applyDiff({
        name: "John Smith",
        age: 31,
        email: "john.smith@example.com",
      });
    });

    const updatedBranchPerson = result.current.branch;
    assertLoaded(updatedBranchPerson);

    // Verify the branch has the changes
    expect(updatedBranchPerson.name).toBe("John Smith");
    expect(updatedBranchPerson.age).toBe(31);
    expect(updatedBranchPerson.email).toBe("john.smith@example.com");

    const mainPerson = result.current.main;
    assertLoaded(mainPerson);

    // Verify the original is unchanged
    expect(mainPerson.name).toBe("John Doe");
    expect(mainPerson.age).toBe(30);
    expect(mainPerson.email).toBe("john@example.com");

    // Merge the branch back
    await branchPerson.$jazz.unstable_merge();

    const updatedMainPerson = result.current.main;
    assertLoaded(updatedMainPerson);

    // Verify the original now has the merged changes
    expect(updatedMainPerson.name).toBe("John Smith");
    expect(updatedMainPerson.age).toBe(31);
    expect(updatedMainPerson.email).toBe("john.smith@example.com");
  });
});
