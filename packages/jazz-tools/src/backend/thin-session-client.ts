/**
 * The client a thin session's transactions are served by (SPEC 8.14): an
 * in-memory replica in the backend, on the schema version the thin client
 * bound, linked to the core over the same kind of socket as the session's
 * own, opened with the prelude the backend admitted for it (the same vouched
 * user session, guest token or trusted backend link, pinned to the same
 * bundle). The core reads and checks for it as the session; the session's
 * staged writes run as the session.
 */
import { backendNative } from "./backend-native.js";
import type { WasmSchema } from "../drivers/types.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import { JazzClient } from "../runtime/client.js";
import type { AppContext } from "../runtime/context.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import type { NativeSelfSignedClientProof } from "../runtime/native-runtime/native-codec.js";
import type { WebSocketConstructor } from "../runtime/native-runtime/websocket.js";
import { parseJwtPayload } from "../runtime/client-session.js";
import { randomNodeUuidBytes } from "../runtime/runtime-entropy.js";
import type { ThinTransactionClient } from "./thin-relay.js";

/** The canonical author of SYSTEM, which a trusted backend client's socket is served as. */
const SYSTEM_AUTHOR = '["urn:jazz:system","system"]';

export interface ThinSessionClientOptions {
  appId: string;
  /** The public schema JSON the thin client bound its connection to. */
  schema: Uint8Array;
  serverUrl: string;
  /** The prelude the backend admitted the session's socket with. */
  prelude: Uint8Array;
  /** A trusted backend client's socket, which names no session. */
  trusted: boolean;
  /** Opens the session's link: a socket sending `prelude` first. */
  webSocket: WebSocketConstructor;
  /**
   * The migrations the app ships (see {@link ClientMigrations}). Without them
   * the replica names the app's version itself, and the core refuses its
   * transactions as a store opened without the app's migrations.
   */
  migrations?: ClientMigrations;
  /**
   * An app that declares encryption: the replica opens with keyless hooks,
   * as the backend's own does (encrypted writes refused, encrypted cells
   * unreadable, plain data normal), still as the session.
   */
  keyless?: { bindings: string; appId: string; environment: string };
}

export function openThinSessionClient(options: ThinSessionClientOptions): ThinTransactionClient {
  const parsed = JSON.parse(new TextDecoder().decode(options.prelude)) as {
    peer_identity?: string;
    auth?: Record<string, unknown>;
  };
  const author = options.trusted ? SYSTEM_AUTHOR : parsed.peer_identity;
  if (!author) throw new Error("a thin session's prelude names its peer identity");
  // The thin client's own version: the core serves its requests pinned to
  // it, as it does an older replica's.
  const schema = (JSON.parse(new TextDecoder().decode(options.schema)) as { tables?: WasmSchema })
    .tables;
  if (!schema) throw new Error("the thin client bound no schema tables");
  const runtime = new NativeRuntimeAdapter(
    backendNative().Db,
    schema,
    randomNodeUuidBytes(),
    new TextEncoder().encode(author),
    1,
    false,
    {
      webSocket: options.webSocket,
      migrations: options.migrations,
      keyless: options.keyless,
      ...(options.trusted
        ? { backendMode: true, readAuthorizationHost: "trusted-serving" as const }
        : { selfSignedClientProof: guestProof(parsed.auth?.jwt_token, author) }),
    },
  );
  const client = JazzClient.connectWithRuntime(
    runtime,
    { appId: options.appId, schema, serverUrl: options.serverUrl } as AppContext,
    { onAuthFailure: () => {} },
  );
  // The socket sends the admitted prelude in place of the one the runtime
  // builds from this.
  client.connectTransport(options.serverUrl, {});
  let closed = false;
  return {
    serve: (request) => client.serveThinTransaction(request),
    async close() {
      if (closed) return;
      closed = true;
      try {
        await client.releaseThinTransactions();
      } finally {
        await client.shutdown();
      }
    },
  };
}

/**
 * A guest's own token, as the proof its client opens under: only a token the
 * guest signed lets a client act as a local-first identity. The core checks
 * the token again on the link.
 */
function guestProof(token: unknown, author: string): NativeSelfSignedClientProof | undefined {
  if (typeof token !== "string") return undefined;
  const audience = parseJwtPayload(token)?.aud;
  const appId = Array.isArray(audience) ? audience[0] : audience;
  if (typeof appId !== "string") return undefined;
  return { token, appId, claimedAuthor: author };
}
