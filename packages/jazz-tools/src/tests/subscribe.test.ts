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
  co,
  cojsonInternals,
} from "../index.js";
import {
  ID,
  Resolved,
  createCoValueObservable,
  subscribeToCoValue,
} from "../internal.js";
import { createJazzTestAccount, setupJazzTestSync } from "../testing.js";
import { setupAccount, waitFor } from "./utils.js";

class ChatRoom extends CoMap {
  messages = co.ref(MessagesList);
  name = co.string;
}

class Message extends CoMap {
  text = co.string;
  reactions = co.ref(ReactionsStream);
  attachment = co.optional.ref(FileStream);
}

class MessagesList extends CoList.Of(co.ref(Message)) {}
class ReactionsStream extends CoFeed.Of(co.string) {}

function createChatRoom(me: Account | Group, name: string) {
  return ChatRoom.create(
    { messages: MessagesList.create([], { owner: me }), name },
    { owner: me },
  );
}

function createMessage(me: Account | Group, text: string) {
  return Message.create(
    { text, reactions: ReactionsStream.create([], { owner: me }) },
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

    const unsubscribe = subscribeToCoValue(
      ChatRoom,
      chatRoom.id,
      { loadAs: meOnSecondPeer },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        id: chatRoom.id,
        messages: null,
        name: "General",
      }),
      expect.any(Function),
    );

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        id: chatRoom.id,
        name: "General",
        messages: expect.any(Array),
      }),
      expect.any(Function),
    );

    updateFn.mockClear();
    chatRoom.name = "Lounge";

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        id: chatRoom.id,
        name: "Lounge",
        messages: expect.any(Array),
      }),
      expect.any(Function),
    );
  });

  it("shouldn't fire updates until the declared load depth isn't reached", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      ChatRoom,
      chatRoom.id,
      {
        loadAs: meOnSecondPeer,
        resolve: {
          messages: true,
        },
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    expect(updateFn).toHaveBeenCalledTimes(1);
    expect(updateFn).toHaveBeenCalledWith(
      expect.objectContaining({
        id: chatRoom.id,
        name: "General",
        messages: expect.any(Array),
      }),
      expect.any(Function),
    );
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
      ChatRoom,
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
      ChatRoom,
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

    const unsubscribe = subscribeToCoValue(
      ChatRoom,
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
    });

    const initialValue = updateFn.mock.lastCall?.[0];
    const initialMessagesList = initialValue?.messages;
    const initialMessage1 = initialValue?.messages[0];
    const initialMessage2 = initialValue?.messages[1];
    const initialMessageReactions = initialValue?.messages[0].reactions;

    message.reactions?.push("👍");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall?.[0];
    expect(lastValue).not.toBe(initialValue);
    expect(lastValue.messages).not.toBe(initialMessagesList);
    expect(lastValue.messages[0]).not.toBe(initialMessage1);
    expect(lastValue.messages[0].reactions).not.toBe(initialMessageReactions);

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
      ChatRoom,
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
    class TestMap extends CoMap {
      value = co.string;
    }

    class TestList extends CoList.Of(co.ref(TestMap)) {}

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
      TestList,
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

  it("should emit when all the items become available", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    class TestList extends CoList.Of(co.ref(TestMap)) {}

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

    let result = null as Resolved<TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();

    const unsubscribe = subscribeToCoValue(
      TestList,
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

  it("should handle null values in lists with required refs", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    class TestList extends CoList.Of(co.ref(TestMap)) {}

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
        // TODO: This should be flagged as an error by typescript
        null,
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      everyone,
    );

    let result = null as Resolved<TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      TestList,
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

  it("should handle null values in lists with optional refs", async () => {
    class TestMap extends CoMap {
      value = co.string;
    }

    class TestList extends CoList.Of(co.optional.ref(TestMap)) {}

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
        null,
        TestMap.create({ value: "2" }, everyone),
        TestMap.create({ value: "3" }, everyone),
        TestMap.create({ value: "4" }, everyone),
        TestMap.create({ value: "5" }, everyone),
      ],
      everyone,
    );

    let result = null as Resolved<TestList, { $each: true }> | null;

    const updateFn = vi.fn().mockImplementation((value) => {
      result = value;
    });
    const onUnauthorized = vi.fn();
    const onUnavailable = vi.fn();

    const unsubscribe = subscribeToCoValue(
      TestList,
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

    expect(result[0]).toBeNull();

    expect(updateFn).toHaveBeenCalledTimes(1);
  });
});

describe("createCoValueObservable", () => {
  class TestMap extends CoMap {
    color = co.string;
  }

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
      TestMap,
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
      TestMap,
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
      TestMap,
      Resolved<TestMap, {}>
    >();

    const unsubscribe = observable.subscribe(
      TestMap,
      "co_z123" as ID<TestMap>,
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
