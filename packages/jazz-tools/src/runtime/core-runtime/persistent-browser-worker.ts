import type { RuntimeSourcesConfig } from "../context.js";
import {
  loadWasmModule,
  type MutationErrorEvent,
  type Runtime,
  type WasmModule,
} from "../client.js";
import { openConfig } from "./direct-codec.js";
import { encodeDirectSchema } from "./direct-schema-codec.js";
import { CoreRuntime } from "./runtime.js";

type RequestMessage = {
  id: number;
  method: string;
  args: unknown[];
};

type OpenMessage = RequestMessage & {
  method: "open";
  args: [
    runtimeSources: RuntimeSourcesConfig | undefined,
    dbName: string,
    schema: unknown,
    node: Uint8Array,
    author: Uint8Array,
  ];
};

let runtime: Runtime | null = null;

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<RequestMessage>) => void) | null;
  postMessage(message: unknown): void;
};

workerScope.onmessage = (event: MessageEvent<RequestMessage>) => {
  void handleMessage(event.data);
};

async function handleMessage(message: RequestMessage): Promise<void> {
  try {
    if (message.method === "open") {
      await openRuntime(message as OpenMessage);
      postResult(message.id, undefined);
      return;
    }

    if (!runtime) {
      throw new Error("Persistent browser core runtime is not open");
    }
    const target = runtime as unknown as Record<string, (...args: unknown[]) => unknown>;

    if (message.method === "executeSubscription") {
      const [handle] = message.args as [number];
      runtime.executeSubscription(handle, (...args: unknown[]) => {
        workerScope.postMessage({ subscription: handle, args });
      });
      postResult(message.id, undefined);
      return;
    }

    if (isWriteMethod(message.method)) {
      const runtimeArgs = message.args.slice(0, -1);
      const result = target[message.method]!.apply(runtime, runtimeArgs) as {
        transactionId: string;
      };
      postResult(message.id, result);
      return;
    }

    if (message.method === "waitForTransaction") {
      const [transactionId, tier] = message.args as [string, string];
      const result = await runtime.waitForTransaction(transactionId, tier);
      postResult(message.id, result);
      return;
    }

    const method = target[message.method];
    if (typeof method !== "function") {
      throw new Error(`Unsupported persistent browser runtime method ${message.method}`);
    }
    const result = await method.apply(runtime, message.args);
    postResult(message.id, result);
  } catch (error) {
    postError(message.id, error);
  }
}

function isWriteMethod(method: string): boolean {
  return (
    method === "insert" ||
    method === "restore" ||
    method === "update" ||
    method === "upsert" ||
    method === "delete"
  );
}

async function openRuntime(message: OpenMessage): Promise<void> {
  const [runtimeSources, dbName, schema, node, author] = message.args;
  const wasmModule = await loadWasmModule(runtimeSources);
  const db = await wasmModule.WasmDb.openBrowser(
    dbName,
    encodeDirectSchema(schema as never),
    openConfig(node, author, 1, true),
  );

  runtime = new CoreRuntime(
    {
      openMemory: () => db as never,
    } as unknown as WasmModule["WasmDb"],
    schema as never,
    node,
    author,
    1,
    true,
  );
  runtime.onMutationError((payload: MutationErrorEvent) => {
    workerScope.postMessage({ event: "mutationError", payload });
  });
  runtime.onAuthFailure((reason: string) => {
    workerScope.postMessage({ event: "authFailure", reason });
  });
}

function postResult(id: number, result: unknown): void {
  workerScope.postMessage({ id, ok: true, result });
}

function postError(id: number, error: unknown): void {
  workerScope.postMessage({
    id,
    ok: false,
    error:
      error instanceof Error
        ? { name: error.name, message: error.message }
        : { message: String(error) },
  });
}
