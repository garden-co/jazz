import { CoID, InviteSecret, RawAccount, RawCoMap, SessionID } from "cojson";
import { CoStreamItem, RawCoStream } from "cojson";
import {
  Account,
  AccountInbox,
  CoMap,
  CoValue,
  CoValueClass,
  CoValueOrZodSchema,
  Group,
  ID,
  InstanceOfSchema,
  activeAccountContext,
  anySchemaToCoSchema,
  loadCoValue,
  zodSchemaToCoSchema,
} from "../internal.js";

type TxKey = `${SessionID}/${number}`;

type MessagesStream = RawCoStream<CoID<InboxMessage<CoValue, any>>>;
type FailedMessagesStream = RawCoStream<{
  errors: string[];
  value: CoID<InboxMessage<CoValue, any>>;
}>;
type TxKeyStream = RawCoStream<TxKey>;
export type InboxRoot = RawCoMap<{
  messages: CoID<MessagesStream>;
  processed: CoID<TxKeyStream>;
  failed: CoID<FailedMessagesStream>;
}>;

export function createInboxRoot(account: Account) {
  if (!account.isLocalNodeOwner) {
    throw new Error("Account is not controlled");
  }

  const rawAccount = account._raw;
  // Inbox root needs to be publicly readable so the ID of its properties (in particular `messages`) can be read.
  const group = rawAccount.core.node.createGroup();
  group.addMember("everyone", "reader");

  // The `messages` property (despite having a public ID) is write-only.
  const messagesGroup = rawAccount.core.node.createGroup();
  messagesGroup.addMember("everyone", "writeOnly");
  const messagesFeed = messagesGroup.createStream<MessagesStream>();

  const inboxRoot = group.createMap<InboxRoot>();
  const processedFeed = group.createStream<TxKeyStream>();
  const failedFeed = group.createStream<FailedMessagesStream>();

  inboxRoot.set("messages", messagesFeed.id);
  inboxRoot.set("processed", processedFeed.id);
  inboxRoot.set("failed", failedFeed.id);

  return {
    id: inboxRoot.id,
  };
}

type InboxMessage<I extends CoValue, O extends CoValue | undefined> = RawCoMap<{
  payload: ID<I>;
  result: ID<O> | undefined;
  processed: boolean;
  error: string | undefined;
}>;

async function createInboxMessage<
  I extends CoValue,
  O extends CoValue | undefined,
>(payload: I, inboxOwner: RawAccount) {
  const group = payload._raw.group;

  if (group instanceof RawAccount) {
    throw new Error("Inbox messages should be owned by a group");
  }

  group.addMember(inboxOwner, "writer");

  const message = group.createMap<InboxMessage<I, O>>({
    payload: payload.id,
    result: undefined,
    processed: false,
    error: undefined,
  });

  await payload._raw.core.waitForSync();
  await message.core.waitForSync();

  return message;
}

export class Inbox {
  account: Account;
  messages: MessagesStream;
  processed: TxKeyStream;
  failed: FailedMessagesStream;
  root: InboxRoot;
  processing = new Set<`${SessionID}/${number}`>();

  private constructor(
    account: Account,
    root: InboxRoot,
    messages: MessagesStream,
    processed: TxKeyStream,
    failed: FailedMessagesStream,
  ) {
    this.account = account;
    this.root = root;
    this.messages = messages;
    this.processed = processed;
    this.failed = failed;
  }

  subscribe<M extends CoValueOrZodSchema, O extends CoValue | undefined>(
    Schema: M,
    callback: (
      message: InstanceOfSchema<M>,
      senderAccountID: ID<Account>,
    ) => Promise<O | undefined | void>,
    options: { retries?: number } = {},
  ) {
    const processed = new Set<`${SessionID}/${number}`>();
    const failed = new Map<`${SessionID}/${number}`, string[]>();
    const node = this.account._raw.core.node;

    this.processed.subscribe((stream) => {
      for (const items of Object.values(stream.items)) {
        for (const item of items) {
          processed.add(item.value as TxKey);
        }
      }
    });

    const { account } = this;
    const { retries = 3 } = options;

    let failTimer: ReturnType<typeof setTimeout> | number | undefined =
      undefined;

    const clearFailTimer = () => {
      clearTimeout(failTimer);
      failTimer = undefined;
    };

    const handleNewMessages = (stream: MessagesStream) => {
      clearFailTimer(); // Stop the failure timers, we're going to process the failed entries anyway

      for (const [sessionID, items] of Object.entries(stream.items) as [
        SessionID,
        CoStreamItem<CoID<InboxMessage<InstanceOfSchema<M>, O>>>[],
      ][]) {
        const accountID = getAccountIDfromSessionID(sessionID);

        if (!accountID) {
          console.warn("Received message from unknown account", sessionID);
          continue;
        }

        for (const item of items) {
          const txKey = `${sessionID}/${item.tx.txIndex}` as const;

          if (!processed.has(txKey) && !this.processing.has(txKey)) {
            this.processing.add(txKey);

            const id = item.value;

            node
              .load(id)
              .then((message) => {
                if (message === "unavailable") {
                  return Promise.reject(
                    new Error("Unable to load inbox message " + id),
                  );
                }

                return loadCoValue(
                  anySchemaToCoSchema(Schema),
                  message.get("payload")!,
                  {
                    loadAs: account,
                  },
                );
              })
              .then((value) => {
                if (!value) {
                  return Promise.reject(
                    new Error("Unable to load inbox message " + id),
                  );
                }

                return callback(value as InstanceOfSchema<M>, accountID);
              })
              .then((result) => {
                const inboxMessage = node
                  .expectCoValueLoaded(item.value)
                  .getCurrentContent() as RawCoMap;

                if (result) {
                  inboxMessage.set("result", result.id);
                }

                inboxMessage.set("processed", true);

                this.processed.push(txKey);
                this.processing.delete(txKey);
              })
              .catch((error) => {
                console.error("Error processing inbox message", error);
                this.processing.delete(txKey);
                const errors = failed.get(txKey) ?? [];

                const stringifiedError = String(error);
                errors.push(stringifiedError);

                let inboxMessage: RawCoMap | undefined;

                try {
                  inboxMessage = node
                    .expectCoValueLoaded(item.value)
                    .getCurrentContent() as RawCoMap;

                  inboxMessage.set("error", stringifiedError);
                } catch (error) {}

                if (errors.length > retries) {
                  inboxMessage?.set("processed", true);
                  this.processed.push(txKey);
                  this.failed.push({ errors, value: item.value });
                } else {
                  failed.set(txKey, errors);
                  if (!failTimer) {
                    failTimer = setTimeout(
                      () => handleNewMessages(stream),
                      100,
                    );
                  }
                }
              });
          }
        }
      }
    };

    const unsubscribe = this.messages.subscribe(handleNewMessages);

    return () => {
      unsubscribe();
      clearFailTimer();
    };
  }

  static async load(account: Account) {
    if (!account.inbox?.inbox) {
      throw new Error("The account has not set up their inbox");
    }

    const node = account._raw.core.node;

    const root = await node.load(account.inbox.inbox as CoID<InboxRoot>);

    if (root === "unavailable") {
      throw new Error("Inbox not found");
    }

    const [messages, processed, failed] = await Promise.all([
      node.load(root.get("messages")!),
      node.load(root.get("processed")!),
      node.load(root.get("failed")!),
    ]);

    if (
      messages === "unavailable" ||
      processed === "unavailable" ||
      failed === "unavailable"
    ) {
      throw new Error("Inbox not found");
    }

    return new Inbox(account, root, messages, processed, failed);
  }
}

export class InboxSender<I extends CoValue, O extends CoValue | undefined> {
  currentAccount: Account;
  owner: RawAccount;
  messages: MessagesStream;

  private constructor(
    currentAccount: Account,
    owner: RawAccount,
    messages: MessagesStream,
  ) {
    this.currentAccount = currentAccount;
    this.owner = owner;
    this.messages = messages;
  }

  getOwnerAccount() {
    return this.owner;
  }

  async sendMessage(
    message: I,
  ): Promise<O extends CoValue ? ID<O> : undefined> {
    const inboxMessage = await createInboxMessage<I, O>(message, this.owner);

    this.messages.push(inboxMessage.id);

    return new Promise((resolve, reject) => {
      inboxMessage.subscribe((message) => {
        if (message.get("processed")) {
          const error = message.get("error");
          if (error) {
            reject(new Error(error));
          } else {
            resolve(
              message.get("result") as O extends CoValue ? ID<O> : undefined,
            );
          }
        }
      });
    });
  }

  static async load<
    I extends CoValue,
    O extends CoValue | undefined = undefined,
  >(inboxOwnerID: ID<Account>, currentAccount?: Account) {
    currentAccount ||= activeAccountContext.get();

    const node = currentAccount._raw.core.node;

    const inboxOwnerRaw = await node.load(
      inboxOwnerID as unknown as CoID<RawAccount>,
    );

    if (inboxOwnerRaw === "unavailable") {
      throw new Error("Failed to load the inbox owner");
    }

    const inboxOwnerInbox = await node.load(inboxOwnerRaw.get("inbox")!);

    if (inboxOwnerInbox === "unavailable") {
      throw new Error("Failed to load the inbox owner's inbox ID");
    }

    if (
      inboxOwnerInbox.group.roleOf(currentAccount._raw.id) !== "reader" &&
      inboxOwnerInbox.group.roleOf(currentAccount._raw.id) !== "writer" &&
      inboxOwnerInbox.group.roleOf(currentAccount._raw.id) !== "admin"
    ) {
      throw new Error(
        "Insufficient permissions to access the inbox, make sure it's publicly readable.",
      );
    }

    const inboxRootId = inboxOwnerInbox.get("inbox") as
      | CoID<InboxRoot>
      | undefined;

    if (!inboxRootId) {
      throw new Error("Inbox owner does not have their inbox setup");
    }

    const inboxRoot = await node.load(inboxRootId);

    if (inboxRoot === "unavailable") {
      throw new Error("Failed to load the inbox root");
    }

    const messages = await node.load(inboxRoot.get("messages")!);

    if (messages === "unavailable") {
      throw new Error("Inbox not found");
    }

    return new InboxSender<I, O>(currentAccount, inboxOwnerRaw, messages);
  }
}

function getAccountIDfromSessionID(sessionID: SessionID) {
  const until = sessionID.indexOf("_session");
  const accountID = sessionID.slice(0, until);

  if (accountID.startsWith("co_z")) {
    return accountID as ID<Account>;
  }

  return;
}
