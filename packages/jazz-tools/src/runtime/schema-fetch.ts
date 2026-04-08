import type { ColumnType, Value as WasmValue, WasmSchema } from "../drivers/types.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { normalizePermissionsForWasm } from "../schema-permissions.js";
import { buildEndpointUrl } from "./sync-transport.js";

export interface FetchStoredWasmSchemaOptions {
  adminSecret: string;
  pathPrefix?: string;
  schemaHash: string;
}

export async function fetchStoredWasmSchema(
  serverUrl: string,
  options: FetchStoredWasmSchemaOptions,
): Promise<{ schema: WasmSchema }> {
  const schemaUrl = buildEndpointUrl(
    serverUrl,
    `/schema/${encodeURIComponent(options.schemaHash)}`,
    options.pathPrefix,
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

  const schema = (await response.json()) as WasmSchema;
  return { schema };
}

export interface FetchStoredSchemasOptions {
  adminSecret: string;
  pathPrefix?: string;
}

export async function fetchSchemaHashes(
  serverUrl: string,
  options: FetchStoredSchemasOptions,
): Promise<{ hashes: string[] }> {
  const response = await fetch(buildEndpointUrl(serverUrl, "/schemas", options.pathPrefix), {
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
  adminSecret: string;
  pathPrefix?: string;
  schema: WasmSchema;
  permissions?: CompiledPermissionsMap;
}

export async function publishStoredSchema(
  serverUrl: string,
  options: PublishStoredSchemaOptions,
): Promise<{ objectId: string; hash: string }> {
  const response = await fetch(buildEndpointUrl(serverUrl, "/admin/schemas", options.pathPrefix), {
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
  adminSecret: string;
  pathPrefix?: string;
}

export async function fetchPermissionsHead(
  serverUrl: string,
  options: FetchPermissionsHeadOptions,
): Promise<{ head: StoredPermissionsHead | null }> {
  const response = await fetch(
    buildEndpointUrl(serverUrl, "/admin/permissions/head", options.pathPrefix),
    {
      method: "GET",
      headers: {
        "X-Jazz-Admin-Secret": options.adminSecret,
      },
    },
  );

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

export interface PublishStoredPermissionsOptions {
  adminSecret: string;
  pathPrefix?: string;
  schemaHash: string;
  permissions: CompiledPermissionsMap;
  expectedParentBundleObjectId?: string | null;
}

export async function publishStoredPermissions(
  serverUrl: string,
  options: PublishStoredPermissionsOptions,
): Promise<{ head: StoredPermissionsHead | null }> {
  const response = await fetch(
    buildEndpointUrl(serverUrl, "/admin/permissions", options.pathPrefix),
    {
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
    },
  );

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
  adminSecret: string;
  pathPrefix?: string;
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
  const response = await fetch(
    buildEndpointUrl(serverUrl, "/admin/migrations", options.pathPrefix),
    {
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
    },
  );

  if (!response.ok) {
    const bodyText = await response.text().catch(() => "");
    const detail = bodyText ? ` - ${bodyText}` : "";
    throw new Error(`Migration push failed: ${response.status} ${response.statusText}${detail}`);
  }

  return (await response.json()) as { objectId: string; fromHash: string; toHash: string };
}
