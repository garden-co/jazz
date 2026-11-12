// Runs in a tab of its own (see session-tab-node.ts): an app's page with a
// Jazz session, as `createJazzClient` opens it there.
import {
  openBrowserSessionCore as createJazzClient,
  type BrowserJazzSession,
} from "../../src/session/create-jazz-session.js";
import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import { isThinDb } from "../../src/runtime/local-first-upgrade.js";
import { sessionTabApp } from "./session-tab-schema.js";

type NodeRequest = (
  url: string,
  init: { method: string; headers: [string, string][]; body: string | null },
) => Promise<{ status: number; headers: [string, string][]; body: string }>;

declare global {
  // Exposed by the Node side: the test backend answers no CORS.
  var __jazzSessionTabRequest: NodeRequest | undefined;
}

let session: BrowserJazzSession | undefined;
// While set, this tab's requests to the backend wait: its session can't
// learn of a sign-in elsewhere until they go on.
let held: { promise: Promise<void>; release: () => void } | undefined;

const nodeFetch: typeof fetch = async (input, init) => {
  const request = new Request(input, init);
  await held?.promise;
  const response = await globalThis.__jazzSessionTabRequest!(request.url, {
    method: request.method,
    headers: [...request.headers],
    body: request.method === "GET" || request.method === "HEAD" ? null : await request.text(),
  });
  return new Response(response.body, { status: response.status, headers: response.headers });
};

/** Open a local-first guest's session at `serverUrl`, and wait for its local-first client. */
export async function openSessionTab(input: {
  serverUrl: string;
  clearOnSignOut?: boolean;
  localFirst?: "now";
}): Promise<void> {
  const pageFetch = globalThis.fetch;
  globalThis.fetch = ((request: RequestInfo | URL, init?: RequestInit) => {
    const url = request instanceof Request ? request.url : String(request);
    return url.startsWith(new URL(input.serverUrl).origin)
      ? nodeFetch(request, init)
      : pageFetch(request, init);
  }) as typeof fetch;
  session = await createJazzClient({
    serverUrl: input.serverUrl,
    fetch: nodeFetch,
    initial: "device",
    clearOnSignOut: input.clearOnSignOut,
    client: async () => ({ createClient: createLocalFirstDb }),
    ...(input.localFirst ? { localFirst: input.localFirst } : {}),
  });
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const client = session.getSnapshot().client;
    if (client && !isThinDb(client.db)) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("The tab did not upgrade to local-first");
}

/** What the tab shows: its session, its link and the tasks it reads. */
export async function sessionTabState(): Promise<{
  status: string;
  account?: string;
  subject?: string;
  link: unknown;
  titles: string[];
}> {
  const snapshot = session!.getSnapshot();
  const db = snapshot.client?.db;
  const titles = db ? (await db.all(sessionTabApp.tasks)).map((task) => task.title).sort() : [];
  return {
    status: snapshot.status,
    account: snapshot.account?.id,
    subject: snapshot.account?.identity?.subject,
    link: db?.getSyncStatus().link,
    titles,
  };
}

/** Insert a task in this tab and wait for the server to have it. */
export async function insertInSessionTab(input: { title: string }): Promise<void> {
  await session!
    .getSnapshot()
    .client!.db.insert(sessionTabApp.tasks, { title: input.title })
    .wait({ tier: "global" });
}

/** Hold this tab's requests to the backend, or let them (and those held) go on. */
export async function holdSessionTabBackend(input: { hold: boolean }): Promise<void> {
  if (input.hold) {
    if (held) return;
    let release!: () => void;
    const promise = new Promise<void>((resolve) => (release = resolve));
    held = { promise, release };
    return;
  }
  held?.release();
  held = undefined;
}

export async function closeSessionTab(): Promise<void> {
  await session?.close();
  session = undefined;
}
