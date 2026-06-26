import type {
  ConnectionManager,
  ConnectionBridgeClientInput,
  ConnectionManagerHost,
} from "./types.js";

/**
 * Manages the connection of a DB that is directly connected to a Jazz server
 */
export class DirectConnectionManager implements ConnectionManager {
  readonly hasDurablePeer = false;

  constructor(private readonly host: ConnectionManagerHost) {}

  async start(): Promise<void> {}

  onClientCreated({ client }: ConnectionBridgeClientInput): void {
    const { config } = this.host;
    if (!config.serverUrl) return;
    client.connectTransport(config.serverUrl, {
      jwt_token: config.jwtToken,
      admin_secret: config.adminSecret,
    });
  }

  async ensureReadyForQuery(): Promise<void> {}

  async ensureReadyForWriteWait(): Promise<void> {}

  updateAuth(): void {}

  sendLifecycleHint(): void {}

  shouldDeferSubscriptionStart(): boolean {
    return false;
  }

  async deleteClientStorage(): Promise<void> {
    throw new Error("deleteClientStorage() is only available in persistent browser DBs.");
  }

  async shutdown(): Promise<void> {}
}
