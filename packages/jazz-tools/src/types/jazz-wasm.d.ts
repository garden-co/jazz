declare module "jazz-wasm" {
  type WasmMutationRejectCode = "permission_denied" | "session_required" | "catalogue_write_denied";
  type WasmObjectOutcomeState =
    | { type: "pending"; mutationId: string }
    | { type: "accepted"; mutationId: string }
    | {
        type: "errored";
        mutationId: string;
        code: WasmMutationRejectCode;
        reason: string;
      };
  type WasmObjectOutcomeEvent = {
    objectId: string;
    outcome: WasmObjectOutcomeState | null;
  };
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

    insert(table: string, values: unknown): { id: string; values: any[] };
    insertDurable(
      table: string,
      values: unknown,
      tier: string,
    ): Promise<{ id: string; values: any[] }>;
    update(objectId: string, values: unknown): void;
    updateDurable(objectId: string, values: unknown, tier: string): Promise<void>;
    delete(objectId: string): void;
    deleteDurable(objectId: string, tier: string): Promise<void>;
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
    setMutationJournalEnabled(enabled: boolean): void;
    listObjectOutcomes(): WasmObjectOutcomeEvent[];
    takeObjectOutcomeEvents(): WasmObjectOutcomeEvent[];
    acknowledgeMutationOutcome(mutationId: string): void;
    addServer(): void;
    removeServer(): void;
    addClient(): string;
    getSchema(): unknown;
    getSchemaHash(): string;
    close?(): void;
    setClientRole?(clientId: string, role: string): void;
    onSyncMessageReceivedFromClient?(clientId: string, messageJson: string): void;
  }
}
