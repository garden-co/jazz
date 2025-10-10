import {
  CoID,
  CoValueCore,
  InviteSecret,
  RawAccount,
  RawCoMap,
  SessionID,
} from "cojson";
import { type AvailableCoValueCore, type RawCoStream } from "cojson";
import {
  type Account,
  CoValue,
  CoValueClassOrSchema,
  CoValueLoadingState,
  ID,
  InstanceOfSchema,
  activeAccountContext,
  coValueClassFromCoValueClassOrSchema,
  loadCoValue,
} from "../internal.js";
import { isCoValueId } from "../lib/id.js";

export type InboxInvite = `${CoID<MessagesStream>}/${InviteSecret}`;
type TxKey = `${SessionID}/${number}`;

type MessagesStream = RawCoStream<CoID<InboxMessage<CoValue, any>>>;
type FailedMessagesStreamItem = {
  errors: string[];
  value: CoID<InboxMessage<CoValue, any>>;
};
type FailedMessagesStream = RawCoStream<FailedMessagesStreamItem>;
type TxKeyStream = RawCoStream<TxKey>;
export type InboxRoot = RawCoMap<{
  messages: CoID<MessagesStream>;
  processed: CoID<TxKeyStream>;
  failed: CoID<FailedMessagesStream>;
  inviteLink: InboxInvite;
}>;

export function createInboxRoot(account: Account) {
  if (!account.$jazz.isLocalNodeOwner) {
    throw new Error("Account is not controlled");
  }

  const rawAccount = account.$jazz.raw;

  const group = rawAccount.core.node.createGroup();
  const messagesFeed = group.createStream<MessagesStream>();

  const inboxRoot = rawAccount.createMap<InboxRoot>();
  const processedFeed = rawAccount.createStream<TxKeyStream>();
  const failedFeed = rawAccount.createStream<FailedMessagesStream>();

  const inviteLink =
    `${messagesFeed.id}/${group.createInvite("writeOnly")}` as const;

  inboxRoot.set("messages", messagesFeed.id);
  inboxRoot.set("processed", processedFeed.id);
  inboxRoot.set("failed", failedFeed.id);

  return {
    id: inboxRoot.id,
    inviteLink,
  };
}

/**
 * An abstraction on top of CoStream to get the new items in a performant way.
 */
class IncrementalFeed {
  constructor(private feed: AvailableCoValueCore) {}

  private sessions = {};
  getNewItems() {
    const items = this.feed.getValidTransactions({
      ignorePrivateTransactions: false,
      from: this.sessions,
    });

    this.sessions = this.feed.knownState().sessions;

    return items;
  }
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
  const group = payload.$jazz.raw.group;

  if (group instanceof RawAccount) {
    throw new Error("Inbox messages should be owned by a group");
  }

  group.addMember(inboxOwner, "writer");

  const message = group.createMap<InboxMessage<I, O>>({
    payload: payload.$jazz.id,
    result: undefined,
    processed: false,
    error: undefined,
  });

  await payload.$jazz.raw.core.waitForSync();
  await message.core.waitForSync();

  return message;
}

class MessageQueue {
  private queue: Array<{
    txKey: TxKey;
    messageId: CoID<InboxMessage<CoValue, any>>;
  }> = [];
  private processing = new Set<TxKey>();
  private concurrencyLimit: number;
  private activeCount = 0;

  constructor(
    concurrencyLimit: number = 10,
    private processMessage: (
      txKey: TxKey,
      messageId: CoID<InboxMessage<CoValue, any>>,
    ) => Promise<void>,
    private handleError: (
      txKey: TxKey,
      messageId: CoID<InboxMessage<CoValue, any>>,
      error: Error,
    ) => void,
  ) {
    this.concurrencyLimit = concurrencyLimit;
  }

  enqueue(txKey: TxKey, messageId: CoID<InboxMessage<CoValue, any>>) {
    this.queue.push({ txKey, messageId });
    this.processNext();
  }

  private async processNext() {
    if (this.activeCount >= this.concurrencyLimit || this.queue.length === 0) {
      return;
    }

    const { txKey, messageId } = this.queue.shift()!;

    if (this.processing.has(txKey)) {
      this.processNext();
      return;
    }

    this.processing.add(txKey);
    this.activeCount++;

    try {
      await this.processMessage(txKey, messageId);
    } catch (error) {
      this.handleError(txKey, messageId, error as Error);
    } finally {
      this.processing.delete(txKey);
      this.activeCount--;
      this.processNext();
    }
  }
}

export class Inbox {
  account: Account;
  messages: MessagesStream;
  processed: TxKeyStream;
  failed: FailedMessagesStream;
  root: InboxRoot;

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

  subscribe<M extends CoValueClassOrSchema, O extends CoValue | undefined>(
    Schema: M,
    callback: (
      message: InstanceOfSchema<M>,
      senderAccountID: ID<Account>,
    ) => Promise<O | undefined | void>,
    options?: { concurrencyLimit?: number },
  ) {
    const processed = new Set<`${SessionID}/${number}`>();
    const node = this.account.$jazz.localNode;

    // Create queue instance inside subscribe function
    const concurrencyLimit = options?.concurrencyLimit ?? 10;

    const processedFeed = new IncrementalFeed(this.processed.core);

    // Track the already processed messages, triggered immediately so we know the messages processed in the previous sessions
    this.processed.subscribe(() => {
      for (const { changes } of processedFeed.getNewItems()) {
        processed.add(changes[0] as TxKey);
      }
    });

    const { account } = this;

    const messagesFeed = new IncrementalFeed(this.messages.core);

    // Set up the message processing handler for the queue
    const processMessage = async (
      txKey: TxKey,
      messageId: CoID<InboxMessage<CoValue, any>>,
    ) => {
      const message = await node.load(messageId);
      if (message === CoValueLoadingState.UNAVAILABLE) {
        throw new Error(`Inbox: message ${messageId} is unavailable`);
      }

      const value = await loadCoValue(
        coValueClassFromCoValueClassOrSchema(Schema),
        message.get("payload")!,
        {
          loadAs: account,
        },
      );

      if (!value) {
        throw new Error(
          `Inbox: Unable to load the payload of message ${messageId}`,
        );
      }

      const accountID = getAccountIDfromSessionID(
        txKey.split("/")[0] as SessionID,
      );
      if (!accountID) {
        throw new Error(`Inbox: Unknown account for message ${messageId}`);
      }

      const result = await callback(value as InstanceOfSchema<M>, accountID);

      const inboxMessage = node
        .expectCoValueLoaded(messageId)
        .getCurrentContent() as RawCoMap;

      if (result) {
        inboxMessage.set("result", result.$jazz.id);
      }

      inboxMessage.set("processed", true);
      this.processed.push(txKey);
    };

    const handleError = (
      txKey: TxKey,
      messageId: CoID<InboxMessage<CoValue, any>>,
      error: Error,
    ) => {
      console.error(error);

      const stringifiedError = String(error);

      this.processed.push(txKey);
      this.failed.push({ errors: [stringifiedError], value: messageId });

      try {
        const inboxMessage = node
          .expectCoValueLoaded(messageId)
          .getCurrentContent() as RawCoMap;

        inboxMessage.set("error", stringifiedError);
        inboxMessage.set("processed", true);
      } catch (error) {}
    };

    const messageQueue = new MessageQueue(
      concurrencyLimit,
      processMessage,
      handleError,
    );

    const handleNewMessages = () => {
      for (const tx of messagesFeed.getNewItems()) {
        const accountID = getAccountIDfromSessionID(tx.txID.sessionID);

        if (!accountID) {
          console.warn(
            "Received message from unknown account",
            tx.txID.sessionID,
          );
          continue;
        }

        const id = tx.changes[0] as CoID<InboxMessage<CoValue, any>>;

        if (!isCoValueId(id)) {
          continue;
        }

        const txKey = `${tx.txID.sessionID}/${tx.txID.txIndex}` as const;

        if (processed.has(txKey)) {
          continue;
        }

        // Enqueue the message for processing
        messageQueue.enqueue(txKey, id);
      }
    };

    const unsubscribe = this.messages.subscribe(handleNewMessages);

    return () => {
      unsubscribe();
    };
  }

  static async load(account: Account) {
    const profile = account.profile;

    if (!profile.$isLoaded) {
      throw new Error("Account profile should already be loaded");
    }

    if (!profile.inbox) {
      throw new Error("The account has not set up their inbox");
    }

    const node = account.$jazz.localNode;

    const root = await node.load(profile.inbox as CoID<InboxRoot>);

    if (root === CoValueLoadingState.UNAVAILABLE) {
      throw new Error("Inbox not found");
    }

    const [messages, processed, failed] = await Promise.all([
      node.load(root.get("messages")!),
      node.load(root.get("processed")!),
      node.load(root.get("failed")!),
    ]);

    if (
      messages === CoValueLoadingState.UNAVAILABLE ||
      processed === CoValueLoadingState.UNAVAILABLE ||
      failed === CoValueLoadingState.UNAVAILABLE
    ) {
      throw new Error("Inbox not found");
    }

    await processed.core.waitForFullStreaming();

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

    const node = currentAccount.$jazz.localNode;

    const inboxOwnerRaw = await node.load(
      inboxOwnerID as unknown as CoID<RawAccount>,
    );

    if (inboxOwnerRaw === CoValueLoadingState.UNAVAILABLE) {
      throw new Error("Failed to load the inbox owner");
    }

    const inboxOwnerProfileRaw = await node.load(inboxOwnerRaw.get("profile")!);

    if (inboxOwnerProfileRaw === CoValueLoadingState.UNAVAILABLE) {
      throw new Error("Failed to load the inbox owner profile");
    }

    if (
      inboxOwnerProfileRaw.group.roleOf(currentAccount.$jazz.raw.id) !==
        "reader" &&
      inboxOwnerProfileRaw.group.roleOf(currentAccount.$jazz.raw.id) !==
        "writer" &&
      inboxOwnerProfileRaw.group.roleOf(currentAccount.$jazz.raw.id) !== "admin"
    ) {
      throw new Error(
        "Insufficient permissions to access the inbox, make sure its user profile is publicly readable.",
      );
    }

    const inboxInvite = inboxOwnerProfileRaw.get("inboxInvite");

    if (!inboxInvite) {
      throw new Error("The user has not set up their inbox");
    }

    const id = await acceptInvite(inboxInvite as InboxInvite, currentAccount);

    const messages = await node.load(id);

    if (messages === CoValueLoadingState.UNAVAILABLE) {
      throw new Error("Inbox not found");
    }

    return new InboxSender<I, O>(currentAccount, inboxOwnerRaw, messages);
  }
}

async function acceptInvite(invite: string, account?: Account) {
  account ||= activeAccountContext.get();

  const id = invite.slice(0, invite.indexOf("/")) as CoID<MessagesStream>;

  const inviteSecret = invite.slice(invite.indexOf("/") + 1) as InviteSecret;

  if (!isCoValueId(id) || !inviteSecret.startsWith("inviteSecret_")) {
    throw new Error("Invalid inbox ticket");
  }

  if (!account.$jazz.isLocalNodeOwner) {
    throw new Error("Account is not controlled");
  }

  await account.$jazz.localNode.acceptInvite(id, inviteSecret);

  return id;
}

function getAccountIDfromSessionID(sessionID: SessionID) {
  const until = sessionID.indexOf("_session");
  const accountID = sessionID.slice(0, until);

  if (isCoValueId(accountID)) {
    return accountID;
  }

  return;
}
