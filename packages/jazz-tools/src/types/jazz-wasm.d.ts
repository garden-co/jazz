declare module "jazz-wasm" {
  type SyncOutboxCallbackArgs =
    | [
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ]
    | [
        err: unknown,
        destinationKind: "server" | "client",
        destinationId: string,
        payloadJson: string,
        isCatalogue: boolean,
      ];
  type SyncOutboxCallback = (...args: SyncOutboxCallbackArgs) => void;

  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;

  export class WasmRuntime {
    constructor(schemaJson: string, appId: string, env: string, userBranch: string, tier?: string);

    insert(table: string, values: unknown): string;
    insertDurable(table: string, values: unknown, tier: string): Promise<string>;
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
