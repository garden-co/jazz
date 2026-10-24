import { StrictMode, useEffect } from "react";
import { createRoot, type Root } from "react-dom/client";
import { JazzProvider, useDb } from "../../src/react/index.js";
import type { RuntimeSourcesConfig } from "../../src/runtime/context.js";
import "../../../../examples/e2ee-chat/quickstart/permissions.js";
import { app } from "../../../../examples/e2ee-chat/quickstart/schema.js";
import { App } from "../../../../examples/e2ee-chat/quickstart/src/App.js";
export interface QuickstartInput {
  appId: string;
  serverUrl: string;
  runtimeSources: RuntimeSourcesConfig;
}

let root: Root | undefined;
let database: ReturnType<typeof useDb> | undefined;

// The UI performs all normal writes and sharing. This probe only checks an
// authoritative nonmember read and an adversarial grant hidden by the UI.
function Probe() {
  const db = useDb();
  useEffect(() => {
    database = db;
    return () => {
      database = undefined;
    };
  }, [db]);
  return null;
}

export function mount(input: QuickstartInput): void {
  if (root) throw new Error("Quickstart is already mounted");
  const element = document.createElement("div");
  document.body.replaceChildren(element);
  root = createRoot(element);
  // Use the documented provider settings and the actual example components,
  // schema and permissions. Only asset URLs and authority address are injected.
  root.render(
    <StrictMode>
      <JazzProvider
        {...input}
        initial="local-first"
        e2ee={{ app }}
        loading={<p>Opening encrypted chat…</p>}
      >
        <App />
        <Probe />
      </JazzProvider>
    </StrictMode>,
  );
}

export async function readRemote(chatId: string) {
  if (!database) throw new Error("Quickstart database is not ready");
  return database.all(app.messages.where({ chatId }), { tier: "remote" });
}

export async function tryGrant(input: { chatId: string; accountId: string }) {
  if (!database) throw new Error("Quickstart database is not ready");
  try {
    await database.e2ee.spaces.grant(app.chats, input.chatId, input.accountId).wait();
    return { unexpectedlyGranted: true };
  } catch (error) {
    return {
      name: error instanceof Error ? error.name : undefined,
      code: error instanceof Error && "code" in error ? error.code : undefined,
      message: String(error),
    };
  }
}

export function unmount(): void {
  root?.unmount();
  root = undefined;
}
