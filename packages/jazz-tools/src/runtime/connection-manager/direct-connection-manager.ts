import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";

/**
 * Manages the connection of a DB that is directly connected to a Jazz server
 */
export class DirectConnectionManager extends ConnectionManager {
  protected readonly hasDurablePeer = false;

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ client }: ConnectionManagerClientInput): void {
    const { config } = this.host;
    if (!config.serverUrl) return;
    client.connectTransport(config.serverUrl, {
      jwt_token: config.jwtToken,
      admin_secret: config.adminSecret,
    });
  }

  async ensureReady(): Promise<void> {}

  sendLifecycleHint(): void {}

  shouldDeferSubscriptionStart(): boolean {
    return false;
  }

  async deleteClientStorage(): Promise<void> {
    throw new Error("deleteClientStorage() is only available in persistent browser DBs.");
  }
}
