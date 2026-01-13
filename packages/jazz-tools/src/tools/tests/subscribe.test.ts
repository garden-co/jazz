import {
  assert,
  beforeEach,
  describe,
  expect,
  it,
  onTestFinished,
  vi,
} from "vitest";
import { Account, Group, cojsonInternals, z } from "../index.js";
import {
  CoValueLoadingState,
  Loaded,
  co,
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
} from "../internal.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { assertLoaded, setupAccount, waitFor } from "./utils.js";
import { getSubscriptionScope } from "../subscribe/index.js";

cojsonInternals.setCoValueLoadingRetryDelay(300);

const ReactionsFeed = co.feed(z.string());

const Message = co.map({
  text: z.string(),
  reactions: ReactionsFeed,
  attachment: co.optional(co.fileStream()),
});

const ChatRoom = co.map({
  messages: co.list(Message),
  name: z.string(),
});

function createChatRoom(me: Account | Group, name: string) {
  return ChatRoom.create(
    { messages: co.list(Message).create([], { owner: me }), name },
    { owner: me },
  );
}

function createMessage(me: Account | Group, text: string) {
  return Message.create(
    { text, reactions: ReactionsFeed.create([], { owner: me }) },
    { owner: me },
  );
}

beforeEach(async () => {
  await setupJazzTestSync();
});

beforeEach(() => {
  cojsonInternals.CO_VALUE_LOADING_CONFIG.MAX_RETRIES = 1;
  cojsonInternals.CO_VALUE_LOADING_CONFIG.TIMEOUT = 1;
});

describe("subscribeToCoValue", () => {
  it("subscribes to a CoMap", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    let result = null as Loaded<typeof ChatRoom, true> | null;

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      { loadAs: meOnSecondPeer },
      (value) => {
        result = value;
        updateFn();
      },
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(result?.$jazz.id).toBe(chatRoom.$jazz.id);
    expect(result?.messages.$jazz.loadingState).toEqual(
      CoValueLoadingState.LOADING,
    );
    expect(result?.name).toBe("General");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(result?.messages).toEqual([]);

    updateFn.mockClear();
    chatRoom.$jazz.set("name", "Lounge");

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(result?.name).toBe("Lounge");
  });

  it("shouldn't fire updates until the declared load depth isn't reached", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    let result = null as Loaded<typeof ChatRoom, {}> | null;

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: true,
        },
      },
      (value) => {
        result = value;
        updateFn();
      },
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(result).toMatchObject({
      $jazz: expect.objectContaining({ id: chatRoom.$jazz.id }),
      name: "General",
      messages: [],
    });
  });

  it("shouldn't fire updates after unsubscribing", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    const { messages } = await chatRoom.$jazz.ensureLoaded({
      resolve: { messages: { $each: true } },
    });

    messages.$jazz.push(createMessage(me, "Hello"));

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: { $each: true },
        },
      },
      updateFn,
    );

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    unsubscribe();
    chatRoom.$jazz.set("name", "Lounge");
    messages.$jazz.push(createMessage(me, "Hello 2"));

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        $jazz: expect.objectContaining({ id: chatRoom.$jazz.id }),
      }),
      expect.any(Function),
    );
  });

  it("should fire updates when a ref entity is updates", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const message = createMessage(
      me,
      "Hello Luigi, are you ready to save the princess?",
    );
    chatRoom.messages?.$jazz.push(message);

    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: {
            $each: true,
          },
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updateFn.mock.lastCall?.[0];

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
    });

    message.$jazz.set("text", "Nevermind, she was gone to the supermarket");
    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall?.[0];
    expect(lastValue?.messages?.[0]?.text).toBe(
      "Nevermind, she was gone to the supermarket",
    );
  });

  it("should handle the updates as immutable changes", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const message = createMessage(
      me,
      "Hello Luigi, are you ready to save the princess?",
    );
    const message2 = createMessage(me, "Let's go!");
    chatRoom.messages?.$jazz.push(message);
    chatRoom.messages?.$jazz.push(message2);

    const updateFn = vi.fn();

    const updates = [] as Loaded<
      typeof ChatRoom,
      {
        messages: {
          $each: {
            reactions: true;
          };
        };
      }
    >[];

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: {
            $each: {
              reactions: true,
            },
          },
        },
      },
      (value) => {
        updates.push(value);
        updateFn();
      },
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updates.at(-1);

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
    });

    const initialValue = updates.at(0);
    const initialMessagesList = initialValue?.messages;
    const initialMessage1 = initialValue?.messages[0];
    const initialMessage2 = initialValue?.messages[1];
    const initialMessageReactions = initialValue?.messages[0]?.reactions;

    message.reactions?.$jazz.push("👍");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updates.at(-1)!;
    expect(lastValue).not.toBe(initialValue);
    expect(lastValue.messages).not.toBe(initialMessagesList);
    expect(lastValue.messages[0]).not.toBe(initialMessage1);
    expect(lastValue.messages[0]?.reactions).not.toBe(initialMessageReactions);

    // This shouldn't change
    expect(lastValue.messages[1]).toBe(initialMessage2);

    // TODO: The initial should point at that snapshot in time
    // expect(lastValue.messages).not.toBe(initialValue.messages);
    // expect(lastValue.messages[0]).not.toBe(initialValue.messages[0]);
    // expect(lastValue.messages[1]).toBe(initialValue.messages[1]);
    // expect(lastValue.messages[0].reactions).not.toBe(initialValue.messages[0].reactions);
  });

  it("should keep the same identity on the ref entities when a property is updated", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const message = createMessage(
      me,
      "Hello Luigi, are you ready to save the princess?",
    );
    const message2 = createMessage(me, "Let's go!");
    chatRoom.messages?.$jazz.push(message);
    chatRoom.messages?.$jazz.push(message2);

    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(ChatRoom),
      chatRoom.$jazz.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: {
            $each: {
              reactions: true,
            },
          },
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updateFn.mock.lastCall?.[0];

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
      expect(lastValue?.messages?.[1]?.text).toBe(message2.text);
    });

    const initialValue = updateFn.mock.lastCall?.[0];
    chatRoom.$jazz.set("name", "Me and Luigi");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall?.[0];
    expect(lastValue).not.toBe(initialValue);
    expect(lastValue.name).toBe("Me and Luigi");

    expect(lastValue.messages).toBe(initialValue.messages);
    expect(lastValue.messages[0]).toBe(initialValue.messages[0]);
    expect(lastValue.messages[1]).toBe(initialValue.messages[1]);
  });

  it("should emit only once when loading a list of values", async () => {
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

    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: account,
        resolve: {
          $each: true,
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await new Promise((resolve) => setTimeout(resolve, 100));

    assert(list);

    expect(list[0]?.value).toBe("1");

    expect(updateFn).toHaveBeenCalledTimes(1);
  });

  it("should emit when all the items become accessible", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const group = Group.create(creator);

    const list = TestList.create(
      [
        TestMap.create({ value: "1" }, group),
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      everyone,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(onUnauthorized).toHaveBeenCalled();
    });

    group.addMember("everyone", "reader");

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(result[0]?.value).toBe("1");

    expect(updateFn).toHaveBeenCalledTimes(1);
  });

  it("should emit when all the items become available", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Disconnect the creator from the sync server
    creator.$jazz.localNode.syncManager
      .getServerPeers(creator.$jazz.raw.id)
      .forEach((peer) => {
        peer.gracefulShutdown();
      });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const list = TestList.create(
      [
        TestMap.create({ value: "1" }, everyone),
        TestMap.create({ value: "2" }, everyone),
      ],
      everyone,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(onUnavailable).toHaveBeenCalled();
    });

    creator.$jazz.localNode.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(result[0]?.value).toBe("1");
    expect(updateFn).toHaveBeenCalledTimes(1);
  });

  it("should handle undefined values in lists with required refs", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const list = TestList.create(
      [
        // @ts-expect-error
        undefined,
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      {
        owner: everyone,
        validation: "loose",
      },
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(onUnavailable).toHaveBeenCalled();
    });

    list.$jazz.set(0, TestMap.create({ value: "1" }, everyone));

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(result[0]?.value).toBe("1");

    expect(updateFn).toHaveBeenCalledTimes(1);
  });

  it("should handle undefined values in lists with optional refs", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(co.optional(TestMap));

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const list = TestList.create(
      [
        undefined,
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      everyone,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(result[0]).toBeUndefined();

    expect(updateFn).toHaveBeenCalledTimes(1);
  });

  it("should unsubscribe from a nested ref when the value is set to undefined", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(co.optional(TestMap));

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const list = TestList.create(
      [
        TestMap.create({ value: "1" }, creator),
        TestMap.create({ value: "2" }, creator),
      ],
      creator,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: creator,
        resolve: {
          $each: true,
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]?.value).toBe("1");
    expect(result[1]?.value).toBe("2");

    const firstItem = result[0]!;

    updateFn.mockClear();

    list.$jazz.set(0, undefined);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]).toBeUndefined();

    updateFn.mockClear();

    firstItem.$jazz.set("value", "3");

    expect(updateFn).not.toHaveBeenCalled();
  });

  it("should unsubscribe from a nested ref when the value is changed to a different ref", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const list = TestList.create(
      [
        TestMap.create({ value: "1" }, creator),
        TestMap.create({ value: "2" }, creator),
      ],
      creator,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: creator,
        resolve: {
          $each: true,
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]?.value).toBe("1");
    expect(result[1]?.value).toBe("2");

    updateFn.mockClear();
    const firstItem = result[0]!;

    // Replace the first item with a new map
    const newMap = TestMap.create({ value: "3" }, creator);
    list.$jazz.set(0, newMap);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]?.value).toBe("3");
    expect(result[1]?.value).toBe("2");

    updateFn.mockClear();

    firstItem.$jazz.set("value", "4");

    expect(updateFn).not.toHaveBeenCalled();

    newMap.$jazz.set("value", "5");

    expect(updateFn).toHaveBeenCalled();
    expect(result[0]?.value).toBe("5");
  });

  it("should emit on group changes, even when the amount of totalValidTransactions doesn't change but the content does", async () => {
    const Person = co.map({
      name: z.string(),
    });

    const creator = await createJazzTestAccount();

    const writer1 = await createJazzTestAccount();
    const writer2 = await createJazzTestAccount();

    const reader = await createJazzTestAccount();

    await Promise.all([
      writer1.$jazz.waitForAllCoValuesSync(),
      writer2.$jazz.waitForAllCoValuesSync(),
      reader.$jazz.waitForAllCoValuesSync(),
    ]);

    const group = Group.create(creator);
    group.addMember(writer1, "writer");
    group.addMember(writer2, "reader");
    group.addMember(reader, "reader");

    const person = Person.create({ name: "creator" }, group);

    await person.$jazz.waitForSync();

    // Disconnect from the sync server, so we can change permissions but not sync them
    creator.$jazz.localNode.syncManager
      .getServerPeers(creator.$jazz.raw.id)
      .forEach((peer) => {
        peer.gracefulShutdown();
      });

    group.removeMember(writer1);
    group.addMember(writer2, "writer");

    let value = null as Loaded<typeof Person, {}> | null;
    const spy = vi.fn((update) => {
      value = update;
    });

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(Person),
      person.$jazz.id,
      {
        loadAs: reader,
      },
      spy,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("creator");

    const personOnWriter1 = await Person.load(person.$jazz.id, {
      loadAs: writer1,
    });

    const personOnWriter2 = await Person.load(person.$jazz.id, {
      loadAs: writer2,
    });

    spy.mockClear();

    assertLoaded(personOnWriter1);
    assertLoaded(personOnWriter2);
    personOnWriter1.$jazz.set("name", "writer1");
    personOnWriter2.$jazz.set("name", "writer2");

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("writer1");
    expect(value?.$jazz.raw.totalValidTransactions).toBe(2);

    spy.mockClear();

    // Reconnect to the sync server
    creator.$jazz.localNode.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("writer2");
    expect(value?.$jazz.raw.totalValidTransactions).toBe(2);
  });

  it("errors on autoloaded values shouldn't block updates", async () => {
    const TestMap = co.map({
      value: z.string(),
    });

    const TestList = co.list(TestMap);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const group = Group.create(creator);

    const list = TestList.create(
      [
        TestMap.create({ value: "1" }, group),
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      everyone,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(TestList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: true,
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    await waitFor(() => {
      assert(result);
      expect(result[1]?.value).toBe("2");
    });

    assert(result);
    expect(result[0]?.$jazz.loadingState).toBe(CoValueLoadingState.LOADING);

    updateFn.mockClear();

    list.$jazz.set(1, TestMap.create({ value: "updated" }, everyone));

    await waitFor(() => {
      expect(result?.[1]?.value).toBe("updated");
    });

    expect(onUnavailable).not.toHaveBeenCalled();
    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  it("errors on autoloaded values shouldn't block updates, even when the error comes from a new ref", async () => {
    const Dog = co.map({
      name: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const PersonList = co.list(Person);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const list = PersonList.create(
      [
        Person.create(
          { name: "Guido", dog: Dog.create({ name: "Giggino" }, everyone) },
          everyone,
        ),
        Person.create(
          { name: "John", dog: Dog.create({ name: "Rex" }, everyone) },
          everyone,
        ),
        Person.create(
          { name: "Jane", dog: Dog.create({ name: "Bella" }, everyone) },
          everyone,
        ),
      ],
      everyone,
    );

    let result = null as Loaded<typeof PersonList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(PersonList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      assert(result?.[0]);
      expect(result[0].name).toBe("Guido");
      assertLoaded(result[0].dog);
      expect(result[0].dog.name).toBe("Giggino");
    });

    await waitFor(() => {
      assert(result?.[1]);
      expect(result[1].name).toBe("John");
      assertLoaded(result[1].dog);
      expect(result[1].dog.name).toBe("Rex");
    });

    await waitFor(() => {
      assert(result?.[2]);
      expect(result[2].name).toBe("Jane");
      assertLoaded(result[2].dog);
      expect(result[2].dog.name).toBe("Bella");
    });

    list[0]!.$jazz.set("dog", Dog.create({ name: "Ninja" }));

    await waitFor(() => {
      expect(result?.[0]?.dog.$jazz.loadingState).toBe(
        CoValueLoadingState.UNAUTHORIZED,
      );
    });

    list[1]!.$jazz.set("dog", Dog.create({ name: "Pinkie" }, everyone));

    await waitFor(() => {
      assert(result?.[1]);
      assertLoaded(result[1].dog);
      expect(result[1].dog.name).toBe("Pinkie");
    });

    expect(onUnavailable).not.toHaveBeenCalled();
    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  it("autoload on $each resolve should work on all items", async () => {
    const Dog = co.map({
      name: z.string(),
    });

    const Person = co.map({
      name: z.string(),
      dog: Dog,
    });

    const PersonList = co.list(Person);

    const reader = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const creator = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    const everyone = Group.create(creator);
    everyone.addMember("everyone", "reader");

    const list = PersonList.create(
      [
        Person.create(
          { name: "Guido", dog: Dog.create({ name: "Giggino" }, everyone) },
          everyone,
        ),
        Person.create(
          { name: "John", dog: Dog.create({ name: "Rex" }, everyone) },
          everyone,
        ),
        Person.create(
          { name: "Jane", dog: Dog.create({ name: "Bella" }, everyone) },
          everyone,
        ),
      ],
      everyone,
    );

    let result = null as Loaded<typeof PersonList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(PersonList),
      list.$jazz.id,
      {
        loadAs: reader,
        resolve: {
          $each: true,
        },
        onUnauthorized,
        onUnavailable,
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      assert(result?.[0]);
      expect(result[0].name).toBe("Guido");
      assertLoaded(result[0].dog);
      expect(result[0].dog.name).toBe("Giggino");
    });

    await waitFor(() => {
      assert(result?.[1]);
      expect(result[1].name).toBe("John");
      assertLoaded(result[1].dog);
      expect(result[1].dog.name).toBe("Rex");
    });

    await waitFor(() => {
      assert(result?.[2]);
      expect(result[2].name).toBe("Jane");
      assertLoaded(result[2].dog);
      expect(result[2].dog.name).toBe("Bella");
    });

    expect(onUnavailable).not.toHaveBeenCalled();
    expect(onUnauthorized).not.toHaveBeenCalled();
  });

  it("should subscribe to a large coValue", async () => {
    const syncServer = await setupJazzTestSync({ asyncPeers: true });

    const Data = co.list(z.string());
    const LargeDataset = co.map({
      metadata: z.object({
        name: z.string(),
        description: z.string(),
        createdAt: z.number(),
      }),
      data: Data,
    });

    const group = Group.create(syncServer);
    const largeMap = LargeDataset.create(
      {
        metadata: {
          name: "Large Dataset",
          description:
            "A dataset with many entries for testing large coValue subscription",
          createdAt: Date.now(),
        },
        data: Data.create([], group),
      },
      group,
    );
    group.addMember("everyone", "reader");

    const dataSize = 100 * 1024;
    const chunkSize = 1024;
    const chunks = dataSize / chunkSize;

    const value = "x".repeat(chunkSize);

    for (let i = 0; i < chunks; i++) {
      largeMap.data.$jazz.push(value);
    }

    // Wait for the large coValue to be fully synced
    await largeMap.data.$jazz.raw.core.waitForSync();

    const alice = await createJazzTestAccount();

    let result = null as Loaded<typeof LargeDataset, { data: true }> | null;
    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });

    // Test subscribing to the large coValue
    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(LargeDataset),
      largeMap.$jazz.id,
      {
        loadAs: alice,
        resolve: {
          data: true,
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(result.metadata.name).toBe("Large Dataset");
    expect(result.metadata.description).toBe(
      "A dataset with many entries for testing large coValue subscription",
    );

    expect(result.data.length).toBe(chunks);
    expect(result.data.$jazz.raw.core.knownState()).toEqual(
      largeMap.data.$jazz.raw.core.knownState(),
    );

    // Test that updates to the large coValue are properly subscribed
    updateFn.mockClear();
    largeMap.data.$jazz.push("new entry");

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(result.data.length).toBe(chunks + 1);
    expect(result.data[chunks]).toBe("new entry");
  });

  it("should emit not emit when the content doesn't bring real changes", async () => {
    const alice = await createJazzTestAccount();

    const Person = co.map({
      name: z.string(),
    });
    const PersonList = co.list(Person);

    const group = Group.create(alice);

    const person1 = Person.create({ name: "John" }, group);
    const person2 = Person.create({ name: "Jane" }, group);

    const personList = PersonList.create([person1, person2], group);

    const spy = vi.fn();
    // Test subscribing to the large coValue
    const unsubscribe = subscribeToCoValue(
      coValueClassFromCoValueClassOrSchema(PersonList),
      personList.$jazz.id,
      {
        loadAs: alice,
        resolve: {
          $each: true,
        },
        syncResolution: true,
      },
      spy,
    );

    onTestFinished(unsubscribe);

    expect(spy).toHaveBeenCalledTimes(1);

    // Test that updates to the large coValue are properly subscribed
    spy.mockClear();

    group.addMember("everyone", "reader");

    await new Promise((resolve) => setTimeout(resolve, 5));

    expect(spy).not.toHaveBeenCalled();
  });

  it.fails(
    "should return the latest loaded state when a deeply loaded child becomes not accessible",
    async () => {
      const Dog = co.map({
        name: z.string(),
      });

      const Person = co.map({
        name: z.string(),
        dog: Dog,
      });

      const reader = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const creator = await createJazzTestAccount({
        isCurrentActiveAccount: true,
      });

      const dogGroup = Group.create(creator);
      dogGroup.addMember(reader, "reader");

      const everyone = Group.create(creator);
      everyone.addMember("everyone", "reader");

      const dog = Dog.create({ name: "Giggino" }, dogGroup);
      const person = Person.create({ name: "Guido", dog }, everyone);

      let result = null as Loaded<typeof Person, { dog: true }> | null;

      const updateFn = vi.fn().mockImplementation((value) => {
        result = value;
      });

      const unsubscribe = subscribeToCoValue(
        coValueClassFromCoValueClassOrSchema(Person),
        person.$jazz.id,
        {
          loadAs: reader,
          resolve: {
            dog: true,
          },
        },
        updateFn,
      );

      onTestFinished(unsubscribe);

      await waitFor(() => {
        assert(result);
        expect(result.name).toBe("Guido");
        assertLoaded(result.dog);
        expect(result.dog.name).toBe("Giggino");
      });

      // Make the dog not accessible by removing the reader from the dog's group
      dogGroup.removeMember(reader);

      await new Promise((resolve) => setTimeout(resolve, 10));

      assert(result);

      // The parent & child loading state should be in sync, but because the child loading state
      // is mutable it becomes not loaded while the parent is still loaded
      expect(result.$isLoaded).toBe(result.dog.$isLoaded);
    },
  );
});

describe("getSubscriptionScope", () => {
  const Person = co.map({
    name: z.string(),
  });
  let person: co.output<typeof Person>;

  beforeEach(async () => {
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
      creationProps: { name: "Hermes Puggington" },
    });

    person = Person.create({ name: "John" });
  });

  describe("when the coValue doesn't have a subscription scope", () => {
    it("creates a new subscription scope", () => {
      expect(person.$jazz._subscriptionScope).toBeUndefined();
      const subscriptionScope = getSubscriptionScope(person);
      expect(subscriptionScope).toBeDefined();
    });

    it("updates the subscription scope in the coValue", () => {
      const subscriptionScope = getSubscriptionScope(person);
      expect(person.$jazz._subscriptionScope).toBeDefined();
      expect(person.$jazz._subscriptionScope).toBe(subscriptionScope);
    });
  });

  describe("when the coValue already has a subscription scope", () => {
    it("returns that subscription scope", async () => {
      const loadedPerson = await Person.load(person.$jazz.id);
      assertLoaded(loadedPerson);
      const subscriptionScope = loadedPerson.$jazz._subscriptionScope;
      expect(subscriptionScope).toBeDefined();
      expect(getSubscriptionScope(loadedPerson)).toBe(subscriptionScope);
    });
  });
});
