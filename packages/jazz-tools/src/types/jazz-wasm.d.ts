declare module "jazz-wasm" {
  type SyncOutboxCallbackArgs =
    | [
        destinationKind: "server" | "client",
        destinationId: string,
        payload: string | Uint8Array,
        isCatalogue: boolean,
      ]
    | [
        err: unknown,
        destinationKind: "server" | "client",
        destinationId: string,
        payload: string | Uint8Array,
        isCatalogue: boolean,
      ];
  type SyncOutboxCallback = (...args: SyncOutboxCallbackArgs) => void;
  type InsertValues = Record<string, unknown>;

  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;

  export class WasmRuntime {
    constructor(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      tier?: string,
      useBinaryEncoding?: boolean,
    );
    schedule?: (task: () => void) => void;

    insert(table: string, values: InsertValues): { id: string; values: any[] };
    insertWithSession(
      table: string,
      values: InsertValues,
      sessionJson?: string | null,
    ): { id: string; values: any[] };
    insertDurable(
      table: string,
      values: InsertValues,
      tier: string,
    ): Promise<{ id: string; values: any[] }>;
    insertDurableWithSession(
      table: string,
      values: InsertValues,
      sessionJson: string | null | undefined,
      tier: string,
    ): Promise<{ id: string; values: any[] }>;
    update(objectId: string, values: unknown): void;
    updateWithSession(objectId: string, values: unknown, sessionJson?: string | null): void;
    updateDurable(objectId: string, values: unknown, tier: string): Promise<void>;
    updateDurableWithSession(
      objectId: string,
      values: unknown,
      sessionJson: string | null | undefined,
      tier: string,
    ): Promise<void>;
    delete(objectId: string): void;
    deleteWithSession(objectId: string, sessionJson?: string | null): void;
    deleteDurable(objectId: string, tier: string): Promise<void>;
    deleteDurableWithSession(
      objectId: string,
      sessionJson: string | null | undefined,
      tier: string,
    ): Promise<void>;
    query(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): Promise<unknown>;
    createSubscription(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    executeSubscription(handle: number, onUpdate: Function): void;
    subscribe(
      queryJson: string,
      onUpdate: Function,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    unsubscribe(handle: number): void;
    onSyncMessageReceived(messageJson: string): void;
    onSyncMessageToSend(callback: SyncOutboxCallback): void;
    addServer(serverCatalogueStateHash?: string | null): void;
    removeServer(): void;
    addClient(): string;
    getSchema(): unknown;
    getSchemaHash(): string;
    close?(): void;
    setClientRole?(clientId: string, role: string): void;
    onSyncMessageReceivedFromClient?(clientId: string, messageJson: string): void;

    // Sync tracer — call enableSyncTracer() first, then read results.
    enableSyncTracer(name?: string): void;
    syncTracerRegisterObject(objectId: string, name: string): void;
    syncTracerMessagesJson(): string | undefined;
    syncTracerDump(): string | undefined;
    syncTracerTally(): string | undefined;
    syncTracerSummary(): string | undefined;
    syncTracerTraceNormalized(): string | undefined;
    syncTracerClear(): void;
    syncTracerCount(): number;
  }
}
