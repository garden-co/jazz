import type { AuthConfig, ConnectRuntimeOptions } from "../runtime/client.js";
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
import { DialedThinCarrier, ThinRuntime, type ThinRuntimeOptions } from "./thin-runtime.js";
import { httpUrlToWs } from "../runtime/url.js";
import { accountConfigBearer } from "../accounts/config-capability.js";
import { bearerWebSocket } from "../runtime/credentialed-transport.js";

/**
 * Runtime source for thin clients: no WASM to load, no worker, no local
 * storage. Every schema client is a {@link ThinRuntime} talking to the server.
 */
export class ThinRuntimeSource extends RuntimeSource<DbConfig> {
  override readonly supportsBrowserWorker = false;
  override readonly supportsPolicyBypass = false;

  /** Dialled as the Db was created, for its runtime to take (`dialAhead`). */
  private dialed: DialedThinCarrier | undefined;
  private dialedTimer: ReturnType<typeof setTimeout> | undefined;

  constructor(private readonly options: ThinRuntimeOptions = {}) {
    super();
  }

  /**
   * Open the connection as the Db is created, before the app's first query
   * brings the schema the runtime needs: its socket, prelude and `Hello`
   * run while the app renders, and the runtime takes it over once it opens
   * the same connection. A connection nobody took within
   * {@link DIALED_UNUSED_MS} closes, so an app that never reads holds none.
   */
  override dialAhead(config: DbConfig, serverUrl: string, auth: AuthConfig): void {
    if (this.dialed) return;
    const dialed = new DialedThinCarrier(
      httpUrlToWs(serverUrl, clientScope(config)),
      JSON.stringify(auth),
      authorOf(config),
      this.webSocketFor(config),
    );
    this.dialed = dialed;
    this.dialedTimer = setTimeout(() => {
      if (this.dialed === dialed) this.dialed = undefined;
      dialed.end();
    }, DIALED_UNUSED_MS);
    // A process waiting only for it isn't kept alive.
    (this.dialedTimer as { unref?: () => void }).unref?.();
  }

  override cancelDialAhead(): void {
    this.takeDialed()?.end();
  }

  override async shutdown(): Promise<void> {
    this.cancelDialAhead();
  }

  private takeDialed(): DialedThinCarrier | undefined {
    clearTimeout(this.dialedTimer);
    const dialed = this.dialed;
    this.dialed = undefined;
    return dialed;
  }

  private webSocketFor(config: DbConfig) {
    // A user the app's backend signed in with its bearer (a host without
    // cookies): the sync socket carries it, as a browser's carries the cookie.
    const bearer =
      config.cookieSession && config.accountId ? accountConfigBearer(config) : undefined;
    return (
      this.options.WebSocket ?? (bearer ? bearerWebSocket(config.serverUrl!, bearer) : undefined)
    );
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
    setTrustedReservedSession(context, getTrustedReservedSession(config));
    const author = authorOf(config);
    const webSocket = this.webSocketFor(config);
    const dialed = this.takeDialed();
    return JazzClient.connectWithRuntime(
      new ThinRuntime(schema, {
        ...this.options,
        ...(webSocket ? { WebSocket: webSocket } : {}),
        ...(author ? { author } : {}),
        ...(dialed ? { dialed } : {}),
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

/** How long a connection dialled ahead waits for its runtime before it closes. */
const DIALED_UNUSED_MS = 30_000;

/** The canonical author a thin client of `config` acts as. */
function authorOf(config: DbConfig): string | undefined {
  const trustedReservedSession = getTrustedReservedSession(config);
  const session = resolveClientInternalSessionSync({ ...config, trustedReservedSession });
  return session
    ? canonicalAuthorSubject(session.issuer, session.user_id, session.account_id)
    : undefined;
}
