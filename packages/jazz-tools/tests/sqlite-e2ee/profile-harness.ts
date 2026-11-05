import { createAccountManager } from "../../src/accounts/create-account-manager.js";
import { createDb } from "../../src/runtime/default-create-db.js";
import type { RuntimeSourcesConfig } from "../../src/runtime/context.js";
import { chatSchema } from "./chat-schema.js";

export interface ProfileInput {
  appId: string;
  serverUrl: string;
  runtimeSources: RuntimeSourcesConfig;
  label: string;
}
export interface ProfileInfo {
  accountId: string;
  restored: boolean;
  previousLabel: string | null;
  secureContext: boolean;
  subtleCrypto: boolean;
}
export interface ChatMessage {
  id: string;
  chatId: string;
  text: string;
}

const { app } = chatSchema();
let db: Awaited<ReturnType<typeof createDb>> | undefined;
let accountId: string | undefined;
let stop: (() => void) | undefined;
let snapshot: ChatMessage[] | undefined;
let failure: Error | undefined;

function database() {
  if (!db) throw new Error("Profile database has not been opened");
  return db;
}

// Every profile creates and restores its own account and keys through the
// shipping browser stores. Only the public account ID leaves this page.
export async function open(input: ProfileInput): Promise<ProfileInfo> {
  if (db) throw new Error("Profile database is already open");
  const previousLabel = localStorage.getItem("sqlite-e2ee-profile-label");
  localStorage.setItem("sqlite-e2ee-profile-label", input.label);
  const accounts = await createAccountManager(input);
  const restored = accounts.getLoggedIn();
  const account = restored ?? accounts.createLocalFirst();
  accountId = account.id;
  db = await createDb({
    appId: input.appId,
    serverUrl: input.serverUrl,
    account,
    runtimeSources: input.runtimeSources,
    // Deliberately identical in all profiles: isolation comes from the browser
    // context, not distinct fixture names or an injected account/key store.
    driver: { type: "persistent", dbName: "sqlite-e2ee-independent-profile" },
    e2ee: { app },
  });
  await db.e2ee.devices.list();
  return {
    accountId: account.id,
    restored: restored !== undefined,
    previousLabel,
    secureContext: isSecureContext,
    subtleCrypto: crypto.subtle !== undefined,
  };
}

export async function createChat(): Promise<{ id: string }> {
  if (!accountId) throw new Error("Profile account is missing");
  return database().insert(app.chats, { ownerId: accountId }).wait({ tier: "global" });
}

export async function send(input: { chatId: string; text: string }): Promise<ChatMessage> {
  return database().insert(app.messages, input).wait({ tier: "global" });
}

export async function grant(input: { chatId: string; accountId: string }): Promise<void> {
  await database().e2ee.spaces.grant(app.chats, input.chatId, input.accountId).wait();
}

export async function read(chatId: string): Promise<ChatMessage[]> {
  return database().all(app.messages.where({ chatId }), { tier: "remote" });
}

export async function readRefusal(chatId: string): Promise<unknown> {
  try {
    return { unexpectedlyRead: await read(chatId) };
  } catch (error) {
    return error instanceof Error && "code" in error ? error.code : String(error);
  }
}

export function watch(chatId: string): void {
  stop?.();
  snapshot = undefined;
  failure = undefined;
  stop = database().subscribe(
    app.messages.where({ chatId }),
    {
      onUpdate: (rows) => {
        snapshot = rows;
      },
      onError: (error) => {
        failure = error;
      },
    },
    { tier: "remote" },
  );
}

export async function waitForMessage(id: string): Promise<ChatMessage[]> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (failure) throw failure;
    if (snapshot?.some((row) => row.id === id)) return snapshot;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("Profile subscription did not receive the accepted message");
}

export async function close(): Promise<void> {
  stop?.();
  stop = undefined;
  await db?.shutdown();
  db = undefined;
}
