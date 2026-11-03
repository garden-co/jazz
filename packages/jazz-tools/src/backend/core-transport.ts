import type { JazzCoreSocketEvent } from "jazz-napi";
import type {
  BrowserWebSocket,
  WebSocketConstructor,
} from "../runtime/native-runtime/websocket.js";
import type { JazzCoreSocket, JazzCoreAccess } from "./core-providers/core-provider.js";
export type CoreSocket = JazzCoreSocket;
export type BackendAccess = JazzCoreAccess;

export function coreResponse(response: {
  status: number;
  headers: string[][];
  body: Uint8Array;
}): Response {
  const headers = new Headers();
  for (const [name, value] of response.headers) headers.append(name!, value!);
  const nullBody = response.status === 204 || response.status === 304;
  return new Response(nullBody ? null : (response.body as BodyInit), {
    status: response.status,
    headers,
  });
}

export function toBytes(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (Array.isArray(data)) return Buffer.concat(data as Uint8Array[]);
  if (typeof data === "string") return new TextEncoder().encode(data);
  throw new Error("unsupported socket message");
}

function arrayBufferOf(bytes: Uint8Array): ArrayBuffer {
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
}

/** A WebSocket-shaped session on the core, for clients in this process. */
export function inProcessWebSocket(
  open: (
    url: string,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
  ) => CoreSocket,
): WebSocketConstructor {
  return class InProcessSocket implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    readyState = 0;
    private readonly listeners = new Map<string, Array<(event?: any) => void>>();
    private session: CoreSocket | undefined;

    constructor(url: string) {
      queueMicrotask(() => {
        if (this.readyState !== 0) return;
        try {
          this.session = open(url, (error, event) => this.onCoreEvent(error, event));
        } catch (error) {
          this.emit("error", error);
          this.finish(1011, error instanceof Error ? error.message : String(error));
          return;
        }
        this.readyState = 1;
        this.emit("open");
      });
    }

    addEventListener(type: string, listener: (event?: any) => void): void {
      const list = this.listeners.get(type) ?? [];
      list.push(listener);
      this.listeners.set(type, list);
    }

    send(data: Uint8Array | string): void {
      if (this.readyState !== 1 || !this.session) throw new Error("socket is not open");
      this.session.send(toBytes(data));
    }

    close(): void {
      if (this.readyState >= 2) return;
      this.session?.close();
      this.finish(1000, "client closed");
    }

    private onCoreEvent(error: Error | null, event: JazzCoreSocketEvent): void {
      if (error) {
        this.emit("error", error);
        this.finish(1011, error.message);
      } else if (event.kind === "message" && event.data) {
        this.emit("message", { data: arrayBufferOf(event.data) });
      } else if (event.kind === "close") {
        this.finish(event.code ?? 1000, event.reason ?? "");
      }
    }

    private emit(type: string, event?: unknown): void {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    private finish(code: number, reason: string): void {
      if (this.readyState >= 2) return;
      this.readyState = 3;
      this.emit("close", { code, reason });
    }
  };
}

/**
 * `socket`, its first frame (the prelude) pinned to the published bundle
 * this backend serves with. The core honours the pin only on this backend's
 * word, so whatever bundle a browser stated is replaced.
 */
export function pinnedSocket(socket: CoreSocket, bundle: () => string | undefined): CoreSocket {
  let first = true;
  return {
    send(data) {
      if (first) {
        first = false;
        const pinned = withBundle(data, bundle());
        // A prelude the pin cannot be stamped into would open unpinned.
        if (pinned === null) {
          socket.close();
          return false;
        }
        data = pinned;
      }
      return socket.send(data);
    },
    close: () => socket.close(),
  };
}

/**
 * `prelude` pinned to `bundle`, or with no pin while none is published yet
 * (the backend's own sockets while it boots): a bundle a browser stated
 * never reaches the core. `null` when the prelude is not a JSON object.
 */
function withBundle(prelude: Uint8Array, bundle: string | undefined): Uint8Array | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(prelude));
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const { bundle: _stated, ...rest } = parsed as Record<string, unknown>;
  return new TextEncoder().encode(
    JSON.stringify(bundle === undefined ? rest : { ...rest, bundle }),
  );
}
