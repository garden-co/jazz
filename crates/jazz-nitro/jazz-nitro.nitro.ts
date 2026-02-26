import { HybridObject } from "react-native-nitro-modules";

/**
 * Full Jazz runtime for React Native via Nitro Rust bridge.
 *
 * Mirrors the Runtime interface from packages/jazz-tools/src/runtime/client.ts.
 * All complex data crosses the FFI boundary as JSON strings.
 */
export interface JazzRuntime extends HybridObject<{ ios: "rust"; android: "rust" }> {
  // --- Lifecycle ---

  /** Initialize the runtime with schema, storage path, and config. */
  open(
    schemaJson: string,
    appId: string,
    env: string,
    userBranch: string,
    dataPath: string,
    tier: string | undefined,
  ): void;

  /** Flush pending writes to durable storage. */
  flush(): void;

  /** Flush and close, releasing filesystem locks. */
  close(): void;

  // --- CRUD ---

  /** Insert a row. Returns the new object ID (UUID string). */
  insert(table: string, valuesJson: string): string;

  /** Partial update of an existing row. */
  update(objectId: string, valuesJson: string): void;

  /** Delete a row by ID. */
  deleteRow(objectId: string): void;

  // --- Queries ---

  /** One-shot query. Returns JSON array of rows. */
  query(
    queryJson: string,
    sessionJson: string | undefined,
    settledTier: string | undefined,
  ): Promise<string>;

  // --- Subscriptions ---

  /** Subscribe to a query. Returns a subscription handle. */
  subscribe(
    queryJson: string,
    onUpdate: (deltaJson: string) => void,
    sessionJson: string | undefined,
    settledTier: string | undefined,
  ): number;

  /** Unsubscribe by handle. */
  unsubscribe(handle: number): void;

  // --- Persisted CRUD ---

  /** Insert and wait for tier acknowledgement. Returns object ID. */
  insertPersisted(table: string, valuesJson: string, tier: string): Promise<string>;

  /** Update and wait for tier acknowledgement. */
  updatePersisted(objectId: string, valuesJson: string, tier: string): Promise<void>;

  /** Delete and wait for tier acknowledgement. */
  deletePersisted(objectId: string, tier: string): Promise<void>;

  // --- Sync ---

  /** Deliver an incoming sync message from a server. */
  onSyncMessageReceived(messageJson: string): void;

  /** Register callback for outbound sync messages. */
  onSyncMessageToSend(callback: (messageJson: string) => void): void;

  /** Deliver an incoming sync message from a client (server mode). */
  onSyncMessageReceivedFromClient(clientId: string, messageJson: string): void;

  // --- Server/Client management ---

  /** Register upstream server for sync. */
  addServer(): void;

  /** Remove upstream server. */
  removeServer(): void;

  /** Register a downstream client. Returns client ID. */
  addClient(): string;

  /** Set a client's role ("user", "admin", or "peer"). */
  setClientRole(clientId: string, role: string): void;

  // --- Scheduling ---

  /** Register callback invoked when Rust needs a batched tick. */
  onBatchedTickNeeded(callback: () => void): void;

  /** Run a batched tick. JS calls this when asked via onBatchedTickNeeded. */
  batchedTick(): void;

  // --- Schema ---

  /** Get the current schema as JSON. */
  getSchemaJson(): string;

  /** Get the schema hash (64-char hex). */
  getSchemaHash(): string;

  // --- Utilities ---

  /** Generate a new UUID v7 ID. */
  generateId(): string;

  /** Current timestamp in milliseconds since epoch. */
  currentTimestampMs(): number;
}
