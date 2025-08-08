import { logger, type Peer } from "cojson";
import { createWebSocketPeer } from "./createWebSocketPeer.js";
import type { AnyWebSocketConstructor } from "./types.js";

export class WebSocketPeerWithReconnection {
  private peer: string;
  private reconnectionTimeout: number;
  private addPeer: (peer: Peer) => void;
  private removePeer: (peer: Peer) => void;
  private WebSocketConstructor: AnyWebSocketConstructor;
  private pingTimeout: number;

  constructor(opts: {
    peer: string;
    reconnectionTimeout: number | undefined;
    addPeer: (peer: Peer) => void;
    removePeer: (peer: Peer) => void;
    WebSocketConstructor?: AnyWebSocketConstructor;
    pingTimeout?: number;
  }) {
    this.peer = opts.peer;
    this.reconnectionTimeout = opts.reconnectionTimeout || 500;
    this.addPeer = opts.addPeer;
    this.removePeer = opts.removePeer;
    this.WebSocketConstructor = opts.WebSocketConstructor || WebSocket;
    this.pingTimeout = opts.pingTimeout || 10_000;
  }

  enabled = false;
  closed = true;

  currentPeer: Peer | undefined = undefined;
  private unsubscribeNetworkChange: (() => void) | undefined = undefined;

  // Basic implementation for environments that don't support network change events (e.g. Node.js)
  // Needs to be extended to handle platform specific APIs
  onNetworkChange(callback: (connected: boolean) => void): () => void {
    callback;
    return () => {};
  }

  private waitForOnline(timeout: number) {
    return new Promise<void>((resolve) => {
      const unsubscribeNetworkChange = this.onNetworkChange((connected) => {
        if (connected) {
          handleTimeoutOrOnline();
        }
      });

      function handleTimeoutOrOnline() {
        clearTimeout(timer);
        unsubscribeNetworkChange?.();
        resolve();
      }

      const timer = setTimeout(handleTimeoutOrOnline, timeout);
    });
  }

  reconnectionAttempts = 0;

  onConnectionChangeListeners = new Set<(connected: boolean) => void>();

  waitUntilConnected = async () => {
    if (this.closed) {
      return new Promise<void>((resolve) => {
        const listener = (connected: boolean) => {
          if (connected) {
            resolve();
            this.onConnectionChangeListeners.delete(listener);
          }
        };

        this.onConnectionChangeListeners.add(listener);
      });
    }
  };

  subscribe = (listener: (connected: boolean) => void) => {
    this.onConnectionChangeListeners.add(listener);
    listener(!this.closed);
  };

  unsubscribe = (listener: (connected: boolean) => void) => {
    this.onConnectionChangeListeners.delete(listener);
  };

  startConnection = async () => {
    if (!this.enabled) return;

    if (this.currentPeer) {
      this.removePeer(this.currentPeer);
      this.currentPeer.outgoing.close();

      this.reconnectionAttempts++;

      const timeout = this.reconnectionTimeout * this.reconnectionAttempts;

      logger.debug(
        `Websocket disconnected, trying to reconnect in ${timeout}ms`,
      );

      await this.waitForOnline(timeout);
    }

    if (!this.enabled) return;

    this.currentPeer = createWebSocketPeer({
      websocket: new this.WebSocketConstructor(this.peer),
      pingTimeout: this.pingTimeout,
      id: this.peer,
      role: "server",
      onClose: () => {
        this.closed = true;
        for (const listener of this.onConnectionChangeListeners) {
          listener(false);
        }
        this.startConnection();
      },
      onSuccess: () => {
        this.closed = false;
        for (const listener of this.onConnectionChangeListeners) {
          listener(true);
        }
        logger.debug("Websocket connection successful");

        this.reconnectionAttempts = 0;
      },
    });

    this.addPeer(this.currentPeer);
  };

  enable = () => {
    if (this.enabled) return;

    this.enabled = true;
    this.startConnection();
  };

  disable = () => {
    if (!this.enabled) return;

    this.enabled = false;

    this.reconnectionAttempts = 0;
    this.unsubscribeNetworkChange?.();
    this.unsubscribeNetworkChange = undefined;

    if (this.currentPeer) {
      this.removePeer(this.currentPeer);
      this.currentPeer.outgoing.close();
      this.currentPeer = undefined;
    }
  };
}
