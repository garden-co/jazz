import {
  assert,
  beforeEach,
  describe,
  expect,
  it,
  onTestFinished,
  vi,
} from "vitest";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  FileStream,
  Group,
  coField,
  cojsonInternals,
  z,
} from "../index.js";
import {
  CoMapInstance,
  ID,
  InstanceOrPrimitiveOfSchema,
  Loaded,
  Resolved,
  co,
  createCoValueObservable,
  subscribeToCoValue,
  zodSchemaToCoSchema,
} from "../internal.js";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing.js";
import { setupAccount, waitFor } from "./utils.js";

cojsonInternals.setCoValueLoadingRetryDelay(300);

const ReactionsFeed = co.feed(z.string());

const Message = co.map({
  text: z.string(),
  reactions: ReactionsFeed,
  attachment: z.optional(co.fileStream()),
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
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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

    expect(result).not.toBeNull();
    expect(result?.id).toBe(chatRoom.id);
    expect(result?.messages).toEqual(null);
    expect(result?.name).toBe("General");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(result?.messages).toEqual([]);

    updateFn.mockClear();
    chatRoom.name = "Lounge";

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
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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
      id: chatRoom.id,
      name: "General",
      messages: [],
    });
  });

  it("shouldn't fire updates after unsubscribing", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    const { messages } = await chatRoom.ensureLoaded({
      resolve: { messages: { $each: true } },
    });

    messages.push(createMessage(me, "Hello"));

    const unsubscribe = subscribeToCoValue(
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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
    chatRoom.name = "Lounge";
    messages.push(createMessage(me, "Hello 2"));

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        id: chatRoom.id,
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
    chatRoom.messages?.push(message);

    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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

    message.text = "Nevermind, she was gone to the supermarket";
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
    chatRoom.messages?.push(message);
    chatRoom.messages?.push(message2);

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
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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

    message.reactions?.push("👍");

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
    chatRoom.messages?.push(message);
    chatRoom.messages?.push(message2);

    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      zodSchemaToCoSchema(ChatRoom),
      chatRoom.id,
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
    chatRoom.name = "Me and Luigi";

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
      zodSchemaToCoSchema(TestList),
      list.id,
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
      zodSchemaToCoSchema(TestList),
      list.id,
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
    creator._raw.core.node.syncManager.getPeers().forEach((peer) => {
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
      zodSchemaToCoSchema(TestList),
      list.id,
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

    creator._raw.core.node.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);

    expect(result[0]?.value).toBe("1");

    // expect(updateFn).toHaveBeenCalledTimes(1);
    // TODO: Getting an extra update here due to https://github.com/garden-co/jazz/issues/2117
    expect(updateFn).toHaveBeenCalledTimes(2);
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
      everyone,
    );

    let result = null as Loaded<typeof TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      zodSchemaToCoSchema(TestList),
      list.id,
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

    list[0] = TestMap.create({ value: "1" }, everyone);

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

    const TestList = co.list(z.optional(TestMap));

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
      zodSchemaToCoSchema(TestList),
      list.id,
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

    const TestList = co.list(z.optional(TestMap));

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
      zodSchemaToCoSchema(TestList),
      list.id,
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

    list[0] = undefined;

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]).toBeUndefined();

    updateFn.mockClear();

    firstItem.value = "3";

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
      zodSchemaToCoSchema(TestList),
      list.id,
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
    list[0] = newMap;

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    assert(result);
    expect(result[0]?.value).toBe("3");
    expect(result[1]?.value).toBe("2");

    updateFn.mockClear();

    firstItem.value = "4";

    expect(updateFn).not.toHaveBeenCalled();

    newMap.value = "5";

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
      writer1.waitForAllCoValuesSync(),
      writer2.waitForAllCoValuesSync(),
      reader.waitForAllCoValuesSync(),
    ]);

    const group = Group.create(creator);
    group.addMember(writer1, "writer");
    group.addMember(writer2, "reader");
    group.addMember(reader, "reader");

    const person = Person.create({ name: "creator" }, group);

    await person.waitForSync();

    // Disconnect from the sync server, so we can change permissions but not sync them
    creator._raw.core.node.syncManager.getPeers().forEach((peer) => {
      peer.gracefulShutdown();
    });

    group.removeMember(writer1);
    group.addMember(writer2, "writer");

    let value = null as Loaded<typeof Person, {}> | null;
    const spy = vi.fn((update) => {
      value = update;
    });

    const unsubscribe = subscribeToCoValue(
      zodSchemaToCoSchema(Person),
      person.id,
      {
        loadAs: reader,
      },
      spy,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("creator");

    const personOnWriter1 = await Person.load(person.id, {
      loadAs: writer1,
    });

    const personOnWriter2 = await Person.load(person.id, {
      loadAs: writer2,
    });

    spy.mockClear();

    assert(personOnWriter1);
    assert(personOnWriter2);
    personOnWriter1.name = "writer1";
    personOnWriter2.name = "writer2";

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("writer1");
    expect(value?._raw.totalValidTransactions).toBe(2);

    spy.mockClear();

    // Reconnect to the sync server
    creator._raw.core.node.syncManager.addPeer(
      getPeerConnectedToTestSyncServer(),
    );

    await waitFor(() => expect(spy).toHaveBeenCalled());
    expect(spy).toHaveBeenCalledTimes(1);
    expect(value?.name).toBe("writer2");
    expect(value?._raw.totalValidTransactions).toBe(2);
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
      zodSchemaToCoSchema(TestList),
      list.id,
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
    expect(result[0]).toBe(null);

    updateFn.mockClear();

    list[1] = TestMap.create({ value: "updated" }, everyone);

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
      zodSchemaToCoSchema(PersonList),
      list.id,
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
      expect(result?.[0]?.name).toBe("Guido");
      expect(result?.[0]?.dog?.name).toBe("Giggino");
    });

    await waitFor(() => {
      expect(result?.[1]?.name).toBe("John");
      expect(result?.[1]?.dog?.name).toBe("Rex");
    });

    await waitFor(() => {
      expect(result?.[2]?.name).toBe("Jane");
      expect(result?.[2]?.dog?.name).toBe("Bella");
    });

    list[0]!.dog = Dog.create({ name: "Ninja" });

    await waitFor(() => {
      expect(result?.[0]?.dog).toBe(null);
    });

    list[1]!.dog = Dog.create({ name: "Pinkie" }, everyone);

    await waitFor(() => {
      expect(result?.[1]?.dog?.name).toBe("Pinkie");
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
      zodSchemaToCoSchema(PersonList),
      list.id,
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
      expect(result?.[0]?.name).toBe("Guido");
      expect(result?.[0]?.dog?.name).toBe("Giggino");
    });

    await waitFor(() => {
      expect(result?.[1]?.name).toBe("John");
      expect(result?.[1]?.dog?.name).toBe("Rex");
    });

    await waitFor(() => {
      expect(result?.[2]?.name).toBe("Jane");
      expect(result?.[2]?.dog?.name).toBe("Bella");
    });

    expect(onUnavailable).not.toHaveBeenCalled();
    expect(onUnauthorized).not.toHaveBeenCalled();
  });
});

describe("createCoValueObservable", () => {
  const TestMap = co.map({
    color: z.string(),
  });

  function createTestMap(me: Account | Group) {
    return TestMap.create({ color: "red" }, { owner: me });
  }

  it("should return undefined when there are no subscribers", async () => {
    const observable = createCoValueObservable();

    expect(observable.getCurrentValue()).toBeUndefined();
  });

  it("should update currentValue when subscribed", async () => {
    const { me, meOnSecondPeer } = await setupAccount();
    const testMap = createTestMap(me);
    const observable = createCoValueObservable();
    const mockListener = vi.fn();

    const unsubscribe = observable.subscribe(
      zodSchemaToCoSchema(TestMap),
      testMap.id,
      {
        loadAs: meOnSecondPeer,
      },
      () => {
        mockListener();
      },
    );

    testMap.color = "blue";

    await waitFor(() => mockListener.mock.calls.length > 0);

    expect(observable.getCurrentValue()).toMatchObject({
      id: testMap.id,
      color: "blue",
    });

    unsubscribe();
  });

  it("should reset to undefined after unsubscribe", async () => {
    const { me, meOnSecondPeer } = await setupAccount();
    const testMap = createTestMap(me);
    const observable = createCoValueObservable();
    const mockListener = vi.fn();

    const unsubscribe = observable.subscribe(
      zodSchemaToCoSchema(TestMap),
      testMap.id,
      {
        loadAs: meOnSecondPeer,
      },
      () => {
        mockListener();
      },
    );

    await waitFor(() => mockListener.mock.calls.length > 0);
    expect(observable.getCurrentValue()).toBeDefined();

    unsubscribe();
    expect(observable.getCurrentValue()).toBeUndefined();
  });

  it("should return null if the coValue is not found", async () => {
    const { meOnSecondPeer } = await setupAccount();
    const observable = createCoValueObservable<
      typeof TestMap,
      Loaded<typeof TestMap, {}>
    >();

    const unsubscribe = observable.subscribe(
      TestMap,
      "co_z123",
      { loadAs: meOnSecondPeer },
      () => {},
    );

    expect(observable.getCurrentValue()).toBeUndefined();

    await waitFor(() => {
      expect(observable.getCurrentValue()).toBeNull();
    });

    unsubscribe();
  });
});
