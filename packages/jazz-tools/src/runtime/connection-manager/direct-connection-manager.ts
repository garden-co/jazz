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
  private isDisconnected = false;
  private reconnectWaiters: Array<() => void> = [];

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {}

  protected override onClientCreated({ client }: ConnectionManagerClientInput): void {
    if (this.isDisconnected) return;
    this.connectClient(client);
  }

  private connectClient(client: ConnectionManagerClientInput["client"]): void {
    const { config } = this.host;
    if (!config.serverUrl) return;
    client.connectTransport(config.serverUrl, {
      jwt_token: config.jwtToken,
      admin_secret: config.adminSecret,
    });
  }

  async ensureReady(): Promise<void> {
    if (!this.isDisconnected) return;
    await new Promise<void>((resolve) => {
      this.reconnectWaiters.push(resolve);
    });
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }

    this.isDisconnected = true;
    this.clientEntry?.client.disconnectTransport();
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }

    this.isDisconnected = false;
    const client = this.clientEntry?.client;
    if (client) {
      this.connectClient(client);
    }
    for (const resolve of this.reconnectWaiters.splice(0)) {
      resolve();
    }
  }

  sendLifecycleHint(): void {}

  shouldDeferSubscriptionStart(): boolean {
    return false;
  }

  async deleteClientStorage(): Promise<void> {
    throw new Error("deleteClientStorage() is only available in persistent browser DBs.");
  }
}
