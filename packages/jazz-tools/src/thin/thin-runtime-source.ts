import type { ConnectRuntimeOptions } from "../runtime/client.js";
import { JazzClient } from "../runtime/client.js";
import { canonicalAuthorSubject } from "../runtime/author-id.js";
import { clientScope } from "../runtime/client-scope.js";
import { resolveClientInternalSessionSync } from "../runtime/client-session.js";
import type { AppContext } from "../runtime/context.js";
import type { DbConfig } from "../runtime/db.js";
import {
  getTrustedReservedSession,
  setTrustedReservedSession,
} from "../runtime/db-internal-session.js";
import {
  RuntimeSource,
  type RuntimeClientContext,
  type RuntimeTokenOptions,
} from "../runtime/runtime-source.js";
import { ANONYMOUS_ISSUER, LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";
import { ThinRuntime, type ThinRuntimeOptions } from "./thin-runtime.js";
import { accountConfigBearer } from "../accounts/config-capability.js";
import { bearerWebSocket } from "../runtime/credentialed-transport.js";

/**
 * Runtime source for thin clients: no WASM to load, no worker, no local
 * storage. Every schema client is a {@link ThinRuntime} talking to the server.
 */
export class ThinRuntimeSource extends RuntimeSource<DbConfig> {
  override readonly supportsBrowserWorker = false;
  override readonly supportsPolicyBypass = false;

  constructor(private readonly options: ThinRuntimeOptions = {}) {
    super();
  }

  override admitConfig(config: DbConfig): void {
    if (!config.serverUrl) {
      throw new Error("A thin client needs a serverUrl: it holds no data of its own");
    }
    if (config.adminSecret) {
      throw new Error("Admin connections are not supported in thin mode");
    }
  }

  override createClient({
    config,
    schema,
    onAuthFailure,
  }: RuntimeClientContext<DbConfig>): JazzClient {
    const runtimeOptions: ConnectRuntimeOptions = { onAuthFailure };
    const context: AppContext = {
      appId: clientScope(config),
      accountId: config.accountId,
      schema,
      driver: { type: "memory" },
      serverUrl: config.serverUrl,
      env: config.env,
      jwtToken: config.jwtToken,
      cookieSession: config.cookieSession,
    };
    const trustedReservedSession = getTrustedReservedSession(config);
    setTrustedReservedSession(context, trustedReservedSession);
    const session = resolveClientInternalSessionSync({ ...config, trustedReservedSession });
    const author = session
      ? canonicalAuthorSubject(session.issuer, session.user_id, session.account_id)
      : undefined;
    // A user the app's backend signed in with its bearer (a host without
    // cookies): the sync socket carries it, as a browser's carries the cookie.
    const bearer =
      config.cookieSession && config.accountId ? accountConfigBearer(config) : undefined;
    const webSocket =
      this.options.WebSocket ?? (bearer ? bearerWebSocket(config.serverUrl!, bearer) : undefined);
    return JazzClient.connectWithRuntime(
      new ThinRuntime(schema, {
        ...this.options,
        ...(webSocket ? { WebSocket: webSocket } : {}),
        ...(author ? { author } : {}),
      }),
      context,
      runtimeOptions,
    );
  }

  override mintLocalFirstToken(options: RuntimeTokenOptions): string {
    return mintSelfSignedToken(LOCAL_FIRST_ISSUER, options);
  }

  override mintAnonymousToken(options: RuntimeTokenOptions): string {
    return mintSelfSignedToken(ANONYMOUS_ISSUER, options);
  }
}
