/**
 * The client a thin session's transactions are served by (SPEC 8.14): an
 * in-memory replica in the backend, linked to the core exactly as the
 * session's own socket is, with the prelude the backend admitted for it:
 * the same vouched user session or guest token, pinned to the same bundle.
 * The core reads and checks for it as the session; the session's staged
 * writes run as the session.
 *
 * A trusted backend client's socket names no session: its client is the
 * backend's, as the core served it as SYSTEM.
 */
import { NapiDb } from "jazz-napi";
import type { WasmSchema } from "../drivers/types.js";
import { JazzClient } from "../runtime/client.js";
import type { AppContext } from "../runtime/context.js";
import { NativeRuntimeAdapter } from "../runtime/native-runtime/native-runtime-adapter.js";
import { randomNodeUuidBytes } from "../runtime/runtime-entropy.js";
import { parseJwtPayload } from "../runtime/client-session.js";
import type { NativeSelfSignedClientProof } from "../runtime/native-runtime/native-codec.js";
import type { ThinTransactionClient } from "./thin-relay.js";

/** The canonical author of SYSTEM, which a trusted backend client's socket is served as. */
const SYSTEM_AUTHOR = '["urn:jazz:system","system"]';

export interface ThinSessionClientOptions {
  appId: string;
  schema: WasmSchema;
  /** The core's URL, as this backend reaches it. */
  serverUrl: string;
  /** The prelude the backend sent the core for the session's socket. */
  prelude: Uint8Array;
  /** The bundle the session's socket is pinned to. */
  bundle?: string;
}

export function openThinSessionClient(options: ThinSessionClientOptions): ThinTransactionClient {
  const parsed = JSON.parse(new TextDecoder().decode(options.prelude)) as {
    peer_identity?: string;
    auth?: Record<string, unknown>;
  };
  const auth = parsed.auth ?? {};
  const trusted = auth.backend_session == null && auth.jwt_token == null;
  const author = trusted ? SYSTEM_AUTHOR : parsed.peer_identity;
  if (!author) throw new Error("a thin session's prelude names its peer identity");
  const runtime = new NativeRuntimeAdapter(
    NapiDb,
    options.schema,
    randomNodeUuidBytes(),
    new TextEncoder().encode(author),
    1,
    false,
    trusted
      ? { backendMode: true, readAuthorizationHost: "trusted-serving" }
      : { selfSignedClientProof: guestProof(auth.jwt_token, author) },
  );
  const client = JazzClient.connectWithRuntime(
    runtime,
    { appId: options.appId, schema: options.schema, serverUrl: options.serverUrl } as AppContext,
    { onAuthFailure: () => {} },
  );
  client.connectTransport(options.serverUrl, {
    ...auth,
    ...(options.bundle === undefined ? {} : { bundle: options.bundle }),
  } as Parameters<JazzClient["connectTransport"]>[1]);
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
