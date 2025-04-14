import { describe, expect, it, vi } from "vitest";
import { Account } from "../coValues/account";
import { CoMap } from "../coValues/coMap";
import { Group } from "../coValues/group";
import { Inbox, InboxSender } from "../coValues/inbox";
import { Profile } from "../exports";
import { co } from "../internal";
import { setupTwoNodes, waitFor } from "./utils";

class Message extends CoMap {
  text = co.string;
}

describe("Inbox", () => {
  describe("Private profile", () => {
    it("Should throw if the inbox owner profile is private", async () => {
      class WorkerAccount extends Account {
        migrate() {
          this.profile = Profile.create(
            { name: "Worker" },
            Group.create({ owner: this }),
          );
        }
      }

      const { clientAccount: sender, serverAccount: receiver } =
        await setupTwoNodes({
          ServerAccountSchema: WorkerAccount,
        });

      await expect(() => InboxSender.load(receiver.id, sender)).rejects.toThrow(
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
    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Message[] = [];
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
    expect(senderAccountID).toBe(sender.id);

    unsubscribe();
  });

  it("should work with empty CoMaps", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    class EmptyMessage extends CoMap {}

    const receiverInbox = await Inbox.load(receiver);

    // Create a message from sender
    const message = EmptyMessage.create(
      {},
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup inbox sender
    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: EmptyMessage[] = [];
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
    expect(receivedMessages[0]?.id).toBe(message.id);
    expect(senderAccountID).toBe(sender.id);

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
        { owner: message._owner },
      );
    });

    // Setup inbox sender
    const inboxSender = await InboxSender.load<Message, Message>(
      receiver.id,
      sender,
    );
    const resultId = await inboxSender.sendMessage(message);

    const result = await Message.load(resultId, { loadAs: receiver });
    expect(result?.text).toBe("Responded from the inbox");

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
    const inboxSender = await InboxSender.load<Message>(receiver.id, sender);
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
    const inboxSender = await InboxSender.load<Message>(receiver.id, sender);

    await expect(inboxSender.sendMessage(message)).rejects.toThrow(
      "Error: Failed",
    );

    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing inbox message",
      expect.any(Error),
    );

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
    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Message[] = [];

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
    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Message[] = [];

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

  it("should retry failed messages", async () => {
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
    const inboxSender = await InboxSender.load(receiver.id, sender);
    const promise = inboxSender.sendMessage(message);

    let failures = 0;

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(
      Message,
      async () => {
        failures++;
        throw new Error("Failed");
      },
      { retries: 2 },
    );

    await expect(promise).rejects.toThrow();
    expect(failures).toBe(3);
    const [failed] = Object.values(receiverInbox.failed.items).flat();
    expect(failed?.value.errors.length).toBe(3);
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing inbox message",
      expect.any(Error),
    );

    errorLogSpy.mockRestore();
  });

  it("should not break the subscription if the message is unavailable", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes();

    const receiverInbox = await Inbox.load(receiver);

    const inboxSender = await InboxSender.load(receiver.id, sender);
    inboxSender.messages.push(`co_z123234` as any);

    const spy = vi.fn();

    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Subscribe to inbox messages
    const unsubscribe = receiverInbox.subscribe(
      Message,
      async () => {
        spy();
      },
      { retries: 2 },
    );

    await waitFor(() => {
      const [failed] = Object.values(receiverInbox.failed.items).flat();

      expect(failed?.value.errors.length).toBe(3);
    });

    expect(spy).not.toHaveBeenCalled();
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing inbox message",
      expect.any(Error),
    );

    errorLogSpy.mockRestore();
  });
});
