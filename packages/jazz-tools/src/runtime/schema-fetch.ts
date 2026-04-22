import type {
  ColumnType,
  TablePolicies,
  Value as WasmValue,
  WasmSchema,
} from "../drivers/types.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { normalizePermissionsForWasm } from "../schema-permissions.js";
import { appScopedUrl } from "./url.js";

export interface FetchStoredWasmSchemaOptions {
  appId: string;
  adminSecret: string;
  schemaHash: string;
}

const MICROSECONDS_PER_MILLISECOND = 1_000;
const EPOCH_MICROSECONDS_THRESHOLD = 100_000_000_000_000;

function normalizePublishedAtEpochMilliseconds(
  publishedAt: number | null | undefined,
): number | null {
  if (typeof publishedAt !== "number" || !Number.isFinite(publishedAt)) {
    return null;
  }

  if (publishedAt >= EPOCH_MICROSECONDS_THRESHOLD) {
    return Math.trunc(publishedAt / MICROSECONDS_PER_MILLISECOND);
  }

  return publishedAt;
}

export async function fetchStoredWasmSchema(
  serverUrl: string,
  options: FetchStoredWasmSchemaOptions,
): Promise<{ schema: WasmSchema; publishedAt: number | null }> {
  const schemaUrl = appScopedUrl(
    serverUrl,
    options.appId,
    `schema/${encodeURIComponent(options.schemaHash)}`,
  );

  const response = await fetch(schemaUrl, {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Schema fetch failed: ${response.status} ${response.statusText}${detail}`);
  }

  const body = (await response.json()) as {
    schema: WasmSchema;
    publishedAt?: number | null;
  };

  return {
    schema: body.schema,
    publishedAt: normalizePublishedAtEpochMilliseconds(body.publishedAt),
  };
}

export interface FetchStoredSchemasOptions {
  appId: string;
  adminSecret: string;
}

export async function fetchSchemaHashes(
  serverUrl: string,
  options: FetchStoredSchemasOptions,
): Promise<{ hashes: string[] }> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "schemas"), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Schema hashes fetch failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const schemaHashesResponse = (await response.json()) as { hashes?: string[] };
  return { hashes: schemaHashesResponse.hashes ?? [] };
}

export interface PublishStoredSchemaOptions {
  appId: string;
  adminSecret: string;
  schema: WasmSchema;
  /** @deprecated Use `publishStoredPermissions` instead. */
  permissions?: CompiledPermissionsMap;
}

export async function publishStoredSchema(
  serverUrl: string,
  options: PublishStoredSchemaOptions,
): Promise<{ objectId: string; hash: string }> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "admin/schemas"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
    body: JSON.stringify({
      schema: options.schema,
      permissions: options.permissions
        ? normalizePermissionsForWasm(options.permissions)
        : undefined,
    }),
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Schema publish failed: ${response.status} ${response.statusText}${detail}`);
  }

  return (await response.json()) as { objectId: string; hash: string };
}

export interface StoredPermissionsHead {
  schemaHash: string;
  version: number;
  parentBundleObjectId: string | null;
  bundleObjectId: string;
}

export interface FetchPermissionsHeadOptions {
  appId: string;
  adminSecret: string;
}

export async function fetchPermissionsHead(
  serverUrl: string,
  options: FetchPermissionsHeadOptions,
): Promise<{ head: StoredPermissionsHead | null }> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "admin/permissions/head"), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Permissions head fetch failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const body = (await response.json()) as { head?: StoredPermissionsHead | null };
  return {
    head: body.head ?? null,
  };
}

export interface StoredPermissionsResponse {
  head: StoredPermissionsHead | null;
  permissions: Record<string, TablePolicies> | null;
}

export interface FetchStoredPermissionsOptions {
  appId: string;
  adminSecret: string;
}

export async function fetchStoredPermissions(
  serverUrl: string,
  options: FetchStoredPermissionsOptions,
): Promise<StoredPermissionsResponse> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "admin/permissions"), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Permissions fetch failed: ${response.status} ${response.statusText}${detail}`);
  }

  const body = (await response.json()) as {
    head?: StoredPermissionsHead | null;
    permissions?: Record<string, TablePolicies> | null;
  };
  return {
    head: body.head ?? null,
    permissions: body.permissions ?? null,
  };
}

export interface PublishStoredPermissionsOptions {
  appId: string;
  adminSecret: string;
  schemaHash: string;
  permissions: CompiledPermissionsMap;
  expectedParentBundleObjectId?: string | null;
}

export async function publishStoredPermissions(
  serverUrl: string,
  options: PublishStoredPermissionsOptions,
): Promise<{ head: StoredPermissionsHead | null }> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "admin/permissions"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
    body: JSON.stringify({
      schemaHash: options.schemaHash,
      permissions: normalizePermissionsForWasm(options.permissions),
      expectedParentBundleObjectId: options.expectedParentBundleObjectId ?? null,
    }),
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Permissions publish failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const body = (await response.json()) as { head?: StoredPermissionsHead | null };
  return {
    head: body.head ?? null,
  };
}

export interface FetchSchemaConnectivityOptions {
  appId: string;
  adminSecret: string;
  fromHash: string;
  toHash: string;
}

export async function fetchSchemaConnectivity(
  serverUrl: string,
  options: FetchSchemaConnectivityOptions,
): Promise<{ connected: boolean }> {
  const url = new URL(appScopedUrl(serverUrl, options.appId, "admin/schema-connectivity"));
  url.searchParams.set("fromHash", options.fromHash);
  url.searchParams.set("toHash", options.toHash);

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(
      `Schema connectivity fetch failed: ${response.status} ${response.statusText}${detail}`,
    );
  }

  const body = (await response.json()) as { connected?: boolean };
  return {
    connected: body.connected ?? false,
  };
}

export type PublishedMigrationValue =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: number }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: number[] }
  | { type: "Array"; value: PublishedMigrationValue[] }
  | { type: "Row"; value: { id?: string; values: PublishedMigrationValue[] } }
  | { type: "Null" };

export type PublishedMigrationOp =
  | {
      type: "introduce";
      column: string;
      column_type: ColumnType;
      value: PublishedMigrationValue;
    }
  | {
      type: "drop";
      column: string;
      column_type: ColumnType;
      value: PublishedMigrationValue;
    }
  | {
      type: "rename";
      column: string;
      value: string;
    };

export interface PublishedTableLens {
  table: string;
  added?: boolean;
  removed?: boolean;
  renamedFrom?: string;
  operations: PublishedMigrationOp[];
}

export interface PublishStoredMigrationOptions {
  appId: string;
  adminSecret: string;
  fromHash: string;
  toHash: string;
  forward: PublishedTableLens[];
}

export function encodePublishedMigrationValue(value: WasmValue): PublishedMigrationValue {
  switch (value.type) {
    case "Bytea":
      return {
        type: "Bytea",
        value: Array.from(value.value),
      };
    case "Array":
      return {
        type: "Array",
        value: value.value.map(encodePublishedMigrationValue),
      };
    case "Row":
      return {
        type: "Row",
        value: {
          id: value.value.id,
          values: value.value.values.map(encodePublishedMigrationValue),
        },
      };
    default:
      return value;
  }
}

export async function publishStoredMigration(
  serverUrl: string,
  options: PublishStoredMigrationOptions,
): Promise<{ objectId: string; fromHash: string; toHash: string }> {
  const response = await fetch(appScopedUrl(serverUrl, options.appId, "admin/migrations"), {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
    body: JSON.stringify({
      fromHash: options.fromHash,
      toHash: options.toHash,
      forward: options.forward,
    }),
  });

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Migration push failed: ${response.status} ${response.statusText}${detail}`);
  }

  return (await response.json()) as { objectId: string; fromHash: string; toHash: string };
}
