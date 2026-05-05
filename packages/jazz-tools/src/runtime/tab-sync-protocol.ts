export interface BroadcastChannelLike {
  postMessage(data: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  close(): void;
}

export interface FollowerSyncMessage {
  type: "follower-sync";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
  payload: Uint8Array[];
}

export interface LeaderSyncMessage {
  type: "leader-sync";
  fromLeaderTabId: string;
  toTabId: string;
  term: number;
  payload: Uint8Array[];
}

export interface FollowerCloseMessage {
  type: "follower-close";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
}

export interface StorageResetRequestMessage {
  type: "storage-reset-request";
  requestId: string;
  fromTabId: string;
  toLeaderTabId: string | null;
  term: number;
}

export interface StorageResetBeginMessage {
  type: "storage-reset-begin";
  requestId: string;
  coordinatorTabId: string;
  term: number;
}

export interface StorageResetAckMessage {
  type: "storage-reset-ack";
  requestId: string;
  fromTabId: string;
  namespace: string;
}

export interface StorageResetFinishedMessage {
  type: "storage-reset-finished";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

export type TabSyncMessage =
  | FollowerSyncMessage
  | LeaderSyncMessage
  | FollowerCloseMessage
  | StorageResetRequestMessage
  | StorageResetBeginMessage
  | StorageResetAckMessage
  | StorageResetFinishedMessage;

export function resolveBroadcastChannelCtor(): (new (name: string) => BroadcastChannelLike) | null {
  const ctor = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor as new (name: string) => BroadcastChannelLike;
}

function isBinaryPayloadArray(value: unknown): value is Uint8Array[] {
  return Array.isArray(value) && value.every((entry) => entry instanceof Uint8Array);
}

export function isTabSyncMessage(value: unknown): value is TabSyncMessage {
  if (typeof value !== "object" || value === null) return false;
  const message = value as Record<string, unknown>;

  if (message.type === "follower-sync") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "leader-sync") {
    return (
      typeof message.fromLeaderTabId === "string" &&
      typeof message.toTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "follower-close") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-request") {
    return (
      typeof message.requestId === "string" &&
      typeof message.fromTabId === "string" &&
      (typeof message.toLeaderTabId === "string" || message.toLeaderTabId === null) &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-begin") {
    return (
      typeof message.requestId === "string" &&
      typeof message.coordinatorTabId === "string" &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-ack") {
    return (
      typeof message.requestId === "string" &&
      typeof message.fromTabId === "string" &&
      typeof message.namespace === "string"
    );
  }

  if (message.type === "storage-reset-finished") {
    return (
      typeof message.requestId === "string" &&
      typeof message.success === "boolean" &&
      (typeof message.errorMessage === "string" || message.errorMessage === undefined)
    );
  }

  return false;
}
