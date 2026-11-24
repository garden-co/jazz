import { createDb as createLocalFirstDb } from "../../src/runtime/default-create-db.js";
import {
  openBrowserSessionCore,
  type BrowserJazzSession,
} from "../../src/session/create-jazz-session.js";
import { app } from "../fixtures/encrypted/jazz.generated.js";
import type { jazzAppRequest } from "./testing-server-node.js";

export type EncryptedTabCommand = { id: string } & (
  | { action: "open"; appId: string; serverUrl: string }
  | { action: "tasks"; projectId: string; insertTitle?: string }
  | { action: "close" }
);

declare module "vitest/browser" {
  interface BrowserCommands {
    encryptedSessionTab(input: EncryptedTabCommand): Promise<string[] | void>;
  }
}

declare global {
  var __jazzEncryptedTabRequest: typeof jazzAppRequest;
}

let session: BrowserJazzSession | undefined;

export async function runEncryptedTabCommand(input: EncryptedTabCommand): Promise<string[] | void> {
  switch (input.action) {
    case "open": {
      const origin = new URL(input.serverUrl).origin;
      const pageFetch = globalThis.fetch;
      const fetch: typeof pageFetch = async (input, init) => {
        const request = new Request(input, init);
        if (new URL(request.url).origin !== origin) return pageFetch(request);
        const response = await globalThis.__jazzEncryptedTabRequest(request.url, {
          method: request.method,
          headers: [...request.headers],
          body: request.body ? await request.text() : null,
        });
        return new Response(response.body, { status: response.status, headers: response.headers });
      };
      globalThis.fetch = fetch;
      const opened = await openBrowserSessionCore({
        scope: input.appId,
        serverUrl: input.serverUrl,
        fetch,
        clearOnSignOut: true,
        localFirst: "now",
        client: async () => ({ createClient: createLocalFirstDb }),
        encryption: { app },
      } as never);
      session = opened;
      if (opened.getSnapshot().status !== "ready") await opened.reauthenticate();
      const status = await opened.getSnapshot().client!.db.encryption.ready({ timeoutMs: 20_000 });
      if (status !== "active") throw new Error(`The second tab's keys are ${status}`);
      return;
    }
    case "tasks": {
      const db = session!.getSnapshot().client!.db;
      if (input.insertTitle !== undefined) {
        await db
          .insert(app.tasks, { projectId: input.projectId, title: input.insertTitle })
          .wait({ tier: "global" });
      }
      const rows = await db.all(app.tasks.where({ projectId: input.projectId }), {
        tier: "local-first",
      });
      return rows.map((row) => row.title).sort();
    }
    case "close":
      await session?.close();
  }
}
