import { createAccountManager, createDb, type AccountStore, type Db } from "jazz-tools";
import jazzWasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";
import { app } from "./schema.js";

const APP_ID = "cloudflare-worker-runtime-ts";

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

// This smoke example deliberately has one disposable account per request.
// Production request handlers should loginJWT against their core registry.
async function openRequestDb(origin: string): Promise<Db> {
  let stored: string | null = null;
  const store: AccountStore = {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
  const runtimeSources = { wasmModule: jazzWasmModule };
  const accounts = await createAccountManager({
    appId: APP_ID,
    serverUrl: origin,
    store,
    runtimeSources,
  });
  const account = accounts.createLocalFirst();
  return createDb({
    appId: APP_ID,
    account,
    driver: { type: "memory" },
    runtimeSources,
  });
}

async function listTodos(db: Db) {
  return db.all(app.todos);
}

async function handleSmoke(db: Db): Promise<Response> {
  const title = `workerd-${crypto.randomUUID().slice(0, 8)}`;
  const { value: inserted } = db.insert(app.todos, {
    title,
    done: false,
  });
  const todos = await listTodos(db);

  return json({
    ok: true,
    runtime: "cloudflare-workers",
    wasmInit: "runtimeSources.wasmModule",
    insertedId: inserted.id,
    todoCount: todos.length,
    todos,
  });
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return json({
        ok: true,
        example: "cloudflare-worker-runtime-ts",
        verify: {
          smoke: "GET /smoke",
        },
      });
    }

    if (url.pathname === "/smoke") {
      const db = await openRequestDb(url.origin);
      try {
        return await handleSmoke(db);
      } finally {
        await db.shutdown();
      }
    }

    return json(
      {
        ok: false,
        error: "Not found",
      },
      404,
    );
  },
};
