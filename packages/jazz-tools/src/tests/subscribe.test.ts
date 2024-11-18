const Crypto = await WasmCrypto.create();
import { connectedPeers } from "cojson/src/streamUtils.js";
import { describe, expect, it, onTestFinished, vi } from "vitest";
import {
  Account,
  CoFeed,
  CoList,
  CoMap,
  WasmCrypto,
  co,
  createJazzContext,
  fixedCredentialsAuth,
  isControlledAccount,
} from "../index.web.js";
import {
  BinaryCoStream,
  Group,
  randomSessionProvider,
  subscribeToCoValue,
} from "../internal.js";

class ChatRoom extends CoMap {
  messages = co.ref(MessagesList);
  name = co.string;
}

class Message extends CoMap {
  text = co.string;
  reactions = co.ref(ReactionsStream);
  attachment = co.optional.ref(BinaryCoStream);
}

class MessagesList extends CoList.Of(co.ref(Message)) {}
class ReactionsStream extends CoFeed.Of(co.string) {}

async function setupAccount() {
  const me = await Account.create({
    creationProps: { name: "Hermes Puggington" },
    crypto: Crypto,
  });

  const [initialAsPeer, secondPeer] = connectedPeers("initial", "second", {
    peer1role: "server",
    peer2role: "client",
  });

  if (!isControlledAccount(me)) {
    throw "me is not a controlled account";
  }
  me._raw.core.node.syncManager.addPeer(secondPeer);
  const { account: meOnSecondPeer } = await createJazzContext({
    auth: fixedCredentialsAuth({
      accountID: me.id,
      secret: me._raw.agentSecret,
    }),
    sessionProvider: randomSessionProvider,
    peersToLoadFrom: [initialAsPeer],
    crypto: Crypto,
  });

  return { me, meOnSecondPeer };
}

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

describe("subscribeToCoValue", () => {
  it("subscribes to a CoMap", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      ChatRoom,
      chatRoom.id,
      meOnSecondPeer,
      {},
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
    );
  });

  it("shouldn't fire updates until the declared load depth isn't reached", async () => {
    const { me, meOnSecondPeer } = await setupAccount();

    const chatRoom = createChatRoom(me, "General");
    const updateFn = vi.fn();

    const unsubscribe = subscribeToCoValue(
      ChatRoom,
      chatRoom.id,
      meOnSecondPeer,
      {
        messages: [],
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
      meOnSecondPeer,
      {
        messages: [{}],
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updateFn.mock.lastCall[0];

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
    });

    message.text = "Nevermind, she was gone to the supermarket";
    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall[0];
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
      meOnSecondPeer,
      {
        messages: [
          {
            reactions: [],
          },
        ],
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updateFn.mock.lastCall[0];

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
    });

    const initialValue = updateFn.mock.lastCall[0];
    const initialMessagesList = initialValue?.messages;
    const initialMessage1 = initialValue?.messages[0];
    const initialMessage2 = initialValue?.messages[1];
    const initialMessageReactions = initialValue?.messages[0].reactions;

    message.reactions?.push("👍");

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall[0];
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
      meOnSecondPeer,
      {
        messages: [
          {
            reactions: [],
          },
        ],
      },
      updateFn,
    );

    onTestFinished(unsubscribe);

    await waitFor(() => {
      const lastValue = updateFn.mock.lastCall[0];

      expect(lastValue?.messages?.[0]?.text).toBe(message.text);
      expect(lastValue?.messages?.[1]?.text).toBe(message2.text);
    });

    const initialValue = updateFn.mock.lastCall[0];
    chatRoom.name = "Me and Luigi";

    updateFn.mockClear();

    await waitFor(() => {
      expect(updateFn).toHaveBeenCalled();
    });

    const lastValue = updateFn.mock.lastCall[0];
    expect(lastValue).not.toBe(initialValue);
    expect(lastValue.name).toBe("Me and Luigi");
    expect(initialValue.name).toBe("General");

    expect(lastValue.messages).toBe(initialValue.messages);
    expect(lastValue.messages[0]).toBe(initialValue.messages[0]);
    expect(lastValue.messages[1]).toBe(initialValue.messages[1]);
  });
});

function waitFor(callback: () => boolean | void) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = () => {
      try {
        return { ok: callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(() => {
      const { ok, error } = checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}
