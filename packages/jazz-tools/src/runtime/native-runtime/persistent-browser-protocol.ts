import type { InsertValues, Value, WasmSchema } from "../../drivers/types.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { NativeRowDelta } from "../../drivers/types.js";

export type PersistentBrowserTelemetryOptions = {
  collectorUrl: string;
  appId: string;
};

type OpenRequest = {
  id: number;
  method: "open";
  args: [
    runtimeSources: RuntimeSourcesConfig | undefined,
    dbName: string,
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
  ];
};

// This protocol exists because OPFS-backed browser storage must be opened and
// used from a dedicated worker. The main thread keeps the Runtime shape and
// proxies calls to the worker that owns the real NativeRuntimeAdapter instance.
export type PersistentBrowserWriteRequest =
  | {
      id: number;
      method: "insert";
      args: [
        table: string,
        values: InsertValues,
        writeContext: string | null | undefined,
        objectId: string,
      ];
    }
  | {
      id: number;
      method: "restore";
      args: [
        table: string,
        objectId: string,
        values: InsertValues,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "update";
      args: [
        table: string,
        objectId: string,
        values: Record<string, Value>,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "upsert";
      args: [
        table: string,
        objectId: string,
        values: InsertValues,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "delete";
      args: [table: string, objectId: string, writeContext: string | null | undefined];
    };

export type PersistentBrowserOpfsOwnerRequest =
  | OpenRequest
  | {
      id: number;
      method: "destroyBrowserStorage";
      args: [runtimeSources: RuntimeSourcesConfig | undefined, dbName: string];
    }
  | PersistentBrowserWriteRequest
  | {
      id: number;
      method: "waitForTransaction";
      args: [transactionId: string, tier: string];
    }
  | {
      id: number;
      method: "beginTransaction";
      args: [kind: "mergeable" | "exclusive"];
    }
  | {
      id: number;
      method: "commitTransaction";
      args: [transactionId: string];
    }
  | {
      id: number;
      method: "rollbackTransaction";
      args: [transactionId: string];
    }
  | {
      id: number;
      method: "query";
      args: [
        queryJson: string,
        sessionJson: string | null | undefined,
        tier: string | null | undefined,
        optionsJson: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "createExecutedSubscription";
      query?: string;
      debugName?: string;
      args: [
        ownerHandle: number,
        queryJson: string,
        sessionJson: string | null | undefined,
        tier: string | null | undefined,
        optionsJson: string | null | undefined,
      ];
    }
  | { id: number; method: "unsubscribe"; args: [handle: number] }
  | { id: number; method: "close"; args: [] }
  | { id: number; method: "closeForStorageClear"; args: [] }
  | {
      id: number;
      method: "installTelemetry";
      args: [telemetry: PersistentBrowserTelemetryOptions];
    }
  | { id: number; method: "connect"; args: [url: string, authJson: string] }
  | { id: number; method: "disconnect"; args: [] }
  | { id: number; method: "updateAuth"; args: [authJson: string] };

export type PersistentBrowserWorkerMethod = PersistentBrowserOpfsOwnerRequest["method"];
type RequestForMethod<Method extends PersistentBrowserWorkerMethod> = Extract<
  PersistentBrowserOpfsOwnerRequest,
  { method: Method }
>;
export type PersistentBrowserRequestArgs<Method extends PersistentBrowserWorkerMethod> =
  RequestForMethod<Method>["args"];

export type PersistentBrowserSubscriptionFrame = {
  kind: "native-row-delta";
  reset?: boolean;
  added: ArrayBuffer;
  removed: ArrayBuffer;
  updated: ArrayBuffer;
  addedCount: number;
  removedCount: number;
  updatedCount: number;
};

export type PersistentBrowserSubscriptionMessage = {
  subscription: number;
} & (
  | { frame: PersistentBrowserSubscriptionFrame }
  | { error: { name?: string; message?: string } }
);

export function isNativeRowDelta(value: unknown): value is NativeRowDelta {
  return (
    !!value &&
    typeof value === "object" &&
    (value as Partial<NativeRowDelta>).__jazzNativeRowDelta === true
  );
}
