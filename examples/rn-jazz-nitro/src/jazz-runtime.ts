import { NitroModules } from "react-native-nitro-modules";

export interface JazzRuntime {
  // Lifecycle
  open(
    schemaJson: string,
    appId: string,
    env: string,
    userBranch: string,
    dataPath: string,
    tier: string | undefined,
  ): void;
  flush(): void;
  close(): void;

  // CRUD
  insert(table: string, valuesJson: string): string;
  update(objectId: string, valuesJson: string): void;
  deleteRow(objectId: string): void;

  // Queries
  query(
    queryJson: string,
    sessionJson: string | undefined,
    settledTier: string | undefined,
  ): Promise<string>;

  // Subscriptions
  subscribe(
    queryJson: string,
    onUpdate: (deltaJson: string) => void,
    sessionJson: string | undefined,
    settledTier: string | undefined,
  ): number;
  unsubscribe(handle: number): void;

  // Scheduling
  onBatchedTickNeeded(callback: () => void): void;
  batchedTick(): void;

  // Schema
  getSchemaJson(): string;
  getSchemaHash(): string;

  // Utilities
  generateId(): string;
  currentTimestampMs(): number;
}

export function getJazzRuntime(): JazzRuntime {
  return NitroModules.createHybridObject<JazzRuntime>("JazzRuntime");
}
