import { beforeEach, describe, expect, it, vi } from "vitest";
import { Group, Inbox, InboxSender, z } from "../exports";
import {
  Account,
  Loaded,
  co,
  coValueClassFromCoValueClassOrSchema,
} from "../internal";
import { setupTwoNodes, waitFor } from "./utils";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing";
import { cojsonInternals, LocalNode } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assertLoaded } from "./utils";

const Message = co.map({
  text: z.string(),
});

beforeEach(async () => {
  await setupJazzTestSync();
  vi.useRealTimers();
});

describe("Inbox", () => {
  describe("Private profile", () => {
    it("Should throw if the inbox owner profile is private", async () => {
      const WorkerAccount = co.account().withMigration((account) => {
        account.$jazz.set(
          "profile",
          co
            .profile()
            .create({ name: "Worker" }, Group.create({ owner: account })),
        );
      });

      const { clientAccount: sender, serverAccount: receiver } =
        await setupTwoNodes({
          ServerAccountSchema:
            coValueClassFromCoValueClassOrSchema(WorkerAccount),
        });

      await expect(() =>
        InboxSender.load(receiver.$jazz.id, sender),
      ).rejects.toThrow(
        "Insufficient permissions to access the inbox, make sure its user profile is publicly readable.",
      );
    });
  });

  it("should create inbox and allow message exchange between accounts", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];
    let senderAccountID: unknown = undefined;

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(
      Message,
      async (message, id) => {
        senderAccountID = id;
        receivedMessages.push(message);
      },
    );

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    expect(receivedMessages.length).toBe(1);
    expect(receivedMessages[0]?.text).toBe("Hello");
    expect(senderAccountID).toBe(sender.$jazz.id);

    unsubscribe();
  });

  it("should work with empty CoMaps", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const EmptyMessage = co.map({});

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = EmptyMessage.create(
      {},
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof EmptyMessage>[] = [];
    let senderAccountID: unknown = undefined;

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(
      EmptyMessage,
      async (message, id) => {
        senderAccountID = id;
        receivedMessages.push(message);
      },
    );

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    expect(receivedMessages.length).toBe(1);
    expect(receivedMessages[0]?.$jazz.id).toBe(message.$jazz.id);
    expect(senderAccountID).toBe(sender.$jazz.id);

    unsubscribe();
  });

  it("should return the result of the message", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const unsubscribe = receiverInbox.subscribe(Message, async (message) => {
      return Message.create(
        { text: "Responded from the inbox" },
        { owner: message.$jazz.owner },
      );
    });

    // Setup inbox sender
    const inboxSender = await InboxSender.load<
      Loaded<typeof Message>,
      Loaded<typeof Message>
    >(receiver.$jazz.id, sender);
    const resultId = await inboxSender.sendMessage(message);

    const result = await Message.load(resultId, { loadAs: receiver });
    assertLoaded(result);
    expect(result.text).toBe("Responded from the inbox");

    unsubscribe();
  });

  it("should return the undefined if the subscription returns undefined", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const unsubscribe = receiverInbox.subscribe(Message, async (message) => {});

    // Setup inbox sender
    const inboxSender = await InboxSender.load<Loaded<typeof Message>>(
      receiver.$jazz.id,
      sender,
    );
    const result = await inboxSender.sendMessage(message);

    expect(result).toBeUndefined();

    unsubscribe();
  });

  it("should reject if the subscription throws an error", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const unsubscribe = receiverInbox.subscribe(Message, async () => {
      return Promise.reject(new Error("Failed"));
    });

    // Setup inbox sender
    const inboxSender = await InboxSender.load<Loaded<typeof Message>>(
      receiver.$jazz.id,
      sender,
    );

    await expect(inboxSender.sendMessage(message)).rejects.toThrow(
      "Error: Failed",
    );

    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(new Error("Failed"));

    errorLogSpy.mockRestore();
  });

  it("should mark messages as processed", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(Message, async (message) => {
      receivedMessages.push(message);
    });

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    inboxSender.sendMessage(message);

    await waitFor(() => receivedMessages.length === 2);

    expect(receivedMessages.length).toBe(2);
    expect(receivedMessages[0]?.text).toBe("Hello");
    expect(receivedMessages[1]?.text).toBe("Hello");

    unsubscribe();
  });

  it("should unsubscribe correctly", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(Message, async (message) => {
      receivedMessages.push(message);
    });

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    unsubscribe();

    inboxSender.sendMessage(message);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(receivedMessages.length).toBe(1);
    expect(receivedMessages[0]?.text).toBe("Hello");
  });

  it("should not retry failed messages", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );
    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    const promise = inboxSender.sendMessage(message);

    let failures = 0;

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(Message, async () => {
      failures++;
      throw new Error("Failed");
    });

    await expect(promise).rejects.toThrow();
    expect(failures).toBe(1);
    const [failed] = Object.values(receiverInbox.failed.items).flat();
    expect(failed?.value.errors.length).toBe(1);
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(new Error("Failed"));

    errorLogSpy.mockRestore();
  });

  it("should not break the subscription if the message is unavailable", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);
    inboxSender.messages.push(`co_z123234` as any);

    const spy = vi.fn();

    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(Message, async () => {
      spy();
    });

    await waitFor(() => {
      const [failed] = Object.values(receiverInbox.failed.items).flat();

      expect(failed?.value.errors.length).toBe(1);
    });

    expect(spy).not.toHaveBeenCalled();
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      new Error("Inbox: message co_z123234 is unavailable"),
    );

    errorLogSpy.mockRestore();
  });

  it("should skip processed messages when a large inbox is restarted", async () => {
    cojsonInternals.setMaxRecommendedTxSize(100);

    const sender = await createJazzTestAccount();
    const receiver = await createJazzTestAccount();

    await receiver.$jazz.waitForAllCoValuesSync();

    const receiverInbox = await Inbox.load(receiver);
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);

    const group = Group.create({ owner: receiver });

    // This generates 4 chunks on the processed stream
    for (let i = 0; i < 5; i++) {
      inboxSender.sendMessage(Message.create({ text: `Hello ${i}` }, group));
    }

    inboxSender.sendMessage(Message.create({ text: `done` }, group));

    await new Promise((resolve) => {
      const unsubscribe = receiverInbox.subscribe(Message, async (message) => {
        if (message.text === "done") {
          resolve(true);
          unsubscribe();
        }
      });
    });

    const accountId = receiver.$jazz.id;
    const accountSecret =
      receiver.$jazz.localNode.getCurrentAgent().agentSecret;
    const sessionID = receiver.$jazz.localNode.currentSessionID;

    await receiver.$jazz.waitForAllCoValuesSync();
    await receiver.$jazz.localNode.gracefulShutdown();

    const node = await LocalNode.withLoadedAccount({
      accountID: accountId as any,
      accountSecret: accountSecret,
      peers: [getPeerConnectedToTestSyncServer()],
      crypto: receiver.$jazz.localNode.crypto,
      sessionID: sessionID,
    });

    const reloadedInbox = await Inbox.load(Account.fromNode(node));

    const subscribeEmitted = await new Promise((resolve) => {
      const unsubscribe = reloadedInbox.subscribe(Message, async (message) => {
        // Got a message
        resolve(true);
      });
      setTimeout(() => {
        resolve(false);
        unsubscribe();
      }, 100);
    });

    expect(subscribeEmitted).toBe(false);
  });

  it("should skip failed messages when a large inbox is restarted", async () => {
    cojsonInternals.setMaxRecommendedTxSize(100);

    const sender = await createJazzTestAccount();
    const receiver = await createJazzTestAccount();

    await receiver.$jazz.waitForAllCoValuesSync();

    const receiverInbox = await Inbox.load(receiver);
    const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);

    const group = Group.create({ owner: receiver });

    // This generates 4 chunks on the processed stream
    for (let i = 0; i < 5; i++) {
      inboxSender
        .sendMessage(Message.create({ text: `Hello ${i}` }, group))
        .catch(() => {});
    }

    inboxSender
      .sendMessage(Message.create({ text: `done` }, group))
      .catch(() => {});

    await new Promise((resolve) => {
      const unsubscribe = receiverInbox.subscribe(Message, async (message) => {
        if (message.text === "done") {
          resolve(true);
          unsubscribe();
        }

        throw new Error("Failed");
      });
    });

    await waitFor(() => {
      expect(Object.values(receiverInbox.processed.items).length).toBe(1);
    });

    const accountId = receiver.$jazz.id;
    const accountSecret =
      receiver.$jazz.localNode.getCurrentAgent().agentSecret;
    const sessionID = receiver.$jazz.localNode.currentSessionID;

    await receiver.$jazz.waitForAllCoValuesSync();
    await receiver.$jazz.localNode.gracefulShutdown();

    const node = await LocalNode.withLoadedAccount({
      accountID: accountId as any,
      accountSecret: accountSecret,
      peers: [getPeerConnectedToTestSyncServer()],
      crypto: receiver.$jazz.localNode.crypto,
      sessionID: sessionID,
    });

    const reloadedInbox = await Inbox.load(Account.fromNode(node));

    const subscribeEmitted = await new Promise((resolve) => {
      const unsubscribe = reloadedInbox.subscribe(Message, async (message) => {
        // Got a message
        resolve(true);
      });
      setTimeout(() => {
        resolve(false);
        unsubscribe();
      }, 100);
    });

    expect(subscribeEmitted).toBe(false);
  });

  describe("Concurrency control", () => {
    it("should respect concurrency limit of 1", async () => {
      const { clientAccount: sender, serverAccount: receiver } =
        await setupTwoNodes();

      const receiverInbox = await Inbox.load(receiver);
      const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);

      const group = Group.create({ owner: sender });

      const processingOrder: string[] = [];

      const Message = co.map({
        text: z.string(),
        value: z.number(),
      });

      // Create messages that take time to process
      const messages = Array.from({ length: 5 }, (_, i) =>
        Message.create({ text: `Message ${i}`, value: i }, group),
      );

      // Subscribe with concurrency limit of 1
      const unsubscribe = receiverInbox.subscribe(
        Message,
        async (message) => {
          const messageText = message.text;
          processingOrder.push(`start-${messageText}`);

          // Simulate processing time
          await new Promise((resolve) =>
            setTimeout(resolve, 20 - message.value * 10),
          );

          processingOrder.push(`end-${messageText}`);
        },
        { concurrencyLimit: 1 },
      );

      // Send all messages at once
      messages.forEach((message) => {
        inboxSender.sendMessage(message);
      });

      // Wait for all messages to be processed
      await waitFor(() => processingOrder.length === 10); // 5 start + 5 end

      expect(processingOrder).toMatchInlineSnapshot(`
        [
          "start-Message 0",
          "end-Message 0",
          "start-Message 1",
          "end-Message 1",
          "start-Message 2",
          "end-Message 2",
          "start-Message 3",
          "end-Message 3",
          "start-Message 4",
          "end-Message 4",
        ]
      `);
      unsubscribe();
    });

    it("should allow concurrent processing with higher concurrency limit", async () => {
      const { clientAccount: sender, serverAccount: receiver } =
        await setupTwoNodes();

      const receiverInbox = await Inbox.load(receiver);
      const inboxSender = await InboxSender.load(receiver.$jazz.id, sender);

      const group = Group.create({ owner: sender });

      // Track processing order and timing
      const processingOrder: string[] = [];

      const Message = co.map({
        text: z.string(),
        value: z.number(),
      });

      // Create messages that take time to process
      const messages = Array.from({ length: 5 }, (_, i) =>
        Message.create({ text: `Message ${i}`, value: i }, group),
      );

      // Subscribe with concurrency limit of 1
      const unsubscribe = receiverInbox.subscribe(
        Message,
        async (message) => {
          const messageText = message.text;
          processingOrder.push(`start-${messageText}`);

          // Simulate processing time
          await new Promise((resolve) =>
            setTimeout(resolve, 20 - message.value * 10),
          );

          processingOrder.push(`end-${messageText}`);
        },
        { concurrencyLimit: 3 },
      );

      // Send all messages at once
      messages.forEach((message) => {
        inboxSender.sendMessage(message);
      });

      await waitFor(() => processingOrder.length === 10);

      expect(processingOrder).toMatchInlineSnapshot(`
        [
          "start-Message 0",
          "start-Message 1",
          "start-Message 2",
          "end-Message 2",
          "start-Message 3",
          "end-Message 3",
          "start-Message 4",
          "end-Message 4",
          "end-Message 1",
          "end-Message 0",
        ]
      `);
      unsubscribe();
    });
  });
});
