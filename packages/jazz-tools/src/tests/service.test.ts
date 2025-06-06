import { describe, expect, it, vi } from "vitest";
import { Group, Service, ServiceSender, z } from "../exports";
import {
  Loaded,
  co,
  createServiceRoot,
  zodSchemaToCoSchema,
} from "../internal";
import { setupTwoNodes, waitFor } from "./utils";

const Message = co.map({
  text: z.string(),
});

const GenericWorkerAccount = co.account({
  service: co.service({}),
  profile: co.profile(),
  root: co.map({}),
});

describe("Service", () => {
  describe("Private service property", () => {
    it("Should throw if the service owner's service property is private", async () => {
      const WorkerAccount = co
        .account({
          service: co.service(),
          profile: co.profile(),
          root: co.map({}),
        })
        .withMigration((account) => {
          if (account.service?.service === undefined) {
            const serviceRoot = createServiceRoot(account);
            account.service = co
              .service()
              .create(
                { service: serviceRoot.id },
                Group.create({ owner: account }),
              );
          }
        });

      const { clientAccount: sender, serverAccount: receiver } =
        await setupTwoNodes({
          ServerAccountSchema: zodSchemaToCoSchema(WorkerAccount),
        });

      await expect(() =>
        ServiceSender.load(receiver.id, sender),
      ).rejects.toThrow(
        "Insufficient permissions to access the service, make sure it's publicly readable.",
      );
    });
  });

  it("should create service and allow message exchange between accounts", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup service sender
    const serviceSender = await ServiceSender.load(receiver.id, sender);
    serviceSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];
    let senderAccountID: unknown = undefined;

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(
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
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const EmptyMessage = co.map({});

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = EmptyMessage.create(
      {},
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup service sender
    const serviceSender = await ServiceSender.load(receiver.id, sender);
    serviceSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof EmptyMessage>[] = [];
    let senderAccountID: unknown = undefined;

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(
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
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const unsubscribe = receiverService.subscribe(Message, async (message) => {
      return Message.create(
        { text: "Responded from the service" },
        { owner: message._owner },
      );
    });

    // Setup service sender
    const serviceSender = await ServiceSender.load<
      Loaded<typeof Message>,
      Loaded<typeof Message>
    >(receiver.id, sender);
    const resultId = await serviceSender.sendMessage(message);

    const result = await Message.load(resultId, { loadAs: receiver });
    expect(result?.text).toBe("Responded from the service");

    unsubscribe();
  });

  it("should return the undefined if the subscription returns undefined", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const unsubscribe = receiverService.subscribe(
      Message,
      async (message) => {},
    );

    // Setup service sender
    const serviceSender = await ServiceSender.load<Loaded<typeof Message>>(
      receiver.id,
      sender,
    );
    const result = await serviceSender.sendMessage(message);

    expect(result).toBeUndefined();

    unsubscribe();
  });

  it("should reject if the subscription throws an error", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const unsubscribe = receiverService.subscribe(Message, async () => {
      return Promise.reject(new Error("Failed"));
    });

    // Setup service sender
    const serviceSender = await ServiceSender.load<Loaded<typeof Message>>(
      receiver.id,
      sender,
    );

    await expect(serviceSender.sendMessage(message)).rejects.toThrow(
      "Error: Failed",
    );

    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing service message",
      expect.any(Error),
    );

    errorLogSpy.mockRestore();
  });

  it("should mark messages as processed", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup service sender
    const serviceSender = await ServiceSender.load(receiver.id, sender);
    serviceSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(Message, async (message) => {
      receivedMessages.push(message);
    });

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    serviceSender.sendMessage(message);

    await waitFor(() => receivedMessages.length === 2);

    expect(receivedMessages.length).toBe(2);
    expect(receivedMessages[0]?.text).toBe("Hello");
    expect(receivedMessages[1]?.text).toBe("Hello");

    unsubscribe();
  });

  it("should unsubscribe correctly", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );

    // Setup service sender
    const serviceSender = await ServiceSender.load(receiver.id, sender);
    serviceSender.sendMessage(message);

    // Track received messages
    const receivedMessages: Loaded<typeof Message>[] = [];

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(Message, async (message) => {
      receivedMessages.push(message);
    });

    // Wait for message to be received
    await waitFor(() => receivedMessages.length === 1);

    unsubscribe();

    serviceSender.sendMessage(message);

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(receivedMessages.length).toBe(1);
    expect(receivedMessages[0]?.text).toBe("Hello");
  });

  it("should retry failed messages", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    // Create a message from sender
    const message = Message.create(
      { text: "Hello" },
      {
        owner: Group.create({ owner: sender }),
      },
    );
    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Setup service sender
    const serviceSender = await ServiceSender.load(receiver.id, sender);
    const promise = serviceSender.sendMessage(message);

    let failures = 0;

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(
      Message,
      async () => {
        failures++;
        throw new Error("Failed");
      },
      { retries: 2 },
    );

    await expect(promise).rejects.toThrow();
    expect(failures).toBe(3);
    const [failed] = Object.values(receiverService.failed.items).flat();
    expect(failed?.value.errors.length).toBe(3);
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing service message",
      expect.any(Error),
    );

    errorLogSpy.mockRestore();
  });

  it("should not break the subscription if the message is unavailable", async () => {
    const { clientAccount: sender, serverAccount: receiver } =
      await setupTwoNodes({
        ServerAccountSchema: zodSchemaToCoSchema(GenericWorkerAccount),
      });

    const receiverService = await Service.load(receiver);

    const serviceSender = await ServiceSender.load(receiver.id, sender);
    serviceSender.messages.push(`co_z123234` as any);

    const spy = vi.fn();

    const errorLogSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Subscribe to service messages
    const unsubscribe = receiverService.subscribe(
      Message,
      async () => {
        spy();
      },
      { retries: 2 },
    );

    await waitFor(() => {
      const [failed] = Object.values(receiverService.failed.items).flat();

      expect(failed?.value.errors.length).toBe(3);
    });

    expect(spy).not.toHaveBeenCalled();
    unsubscribe();

    expect(errorLogSpy).toHaveBeenCalledWith(
      "Error processing service message",
      expect.any(Error),
    );

    errorLogSpy.mockRestore();
  });
});
