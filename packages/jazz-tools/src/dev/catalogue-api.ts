import { fetchFor } from "../runtime/in-process-transport.js";
import { runtimeSchemaJsonReplacer } from "../drivers/schema-wire.js";
import type {
  ColumnType,
  TablePolicies,
  Value as WasmValue,
  WasmSchema,
} from "../drivers/types.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { normalizePermissionsForWasm } from "../schema-permissions.js";
import { appScopedUrl, fetchAdminJson } from "../runtime/url.js";

export interface FetchStoredWasmSchemaOptions {
  appId: string;
  adminSecret: string;
  schemaHash: string;
}

function publishedAtEpochMilliseconds(publishedAt: number | null | undefined): number | null {
  if (typeof publishedAt !== "number" || !Number.isFinite(publishedAt)) {
    return null;
  }

  return publishedAt;
}

export async function fetchStoredWasmSchema(
  serverUrl: string,
  options: FetchStoredWasmSchemaOptions,
): Promise<{ schema: WasmSchema; publishedAt: number | null }> {
  const body = (await fetchAdminJson(
    appScopedUrl(serverUrl, options.appId, `schema/${encodeURIComponent(options.schemaHash)}`),
    options.adminSecret,
    "Schema",
  )) as {
    schema: { tables: WasmSchema };
    publishedAt?: number | null;
  };

  return {
    schema: body.schema.tables,
    publishedAt: publishedAtEpochMilliseconds(body.publishedAt),
  };
}

export interface FetchStoredSchemasOptions {
  appId: string;
  adminSecret: string;
}

export interface StoredSchemaHash {
  hash: string;
  publishedAt: number | null;
}

interface SchemaHashesResponseBody {
  hashes?: string[];
  schemas?: Array<{
    hash: string;
    publishedAt?: number | null;
  }>;
}

function isSchemaHashesResponseBody(value: unknown): value is SchemaHashesResponseBody {
  if (typeof value !== "object" || value === null) {
    return false;
  }

  const candidate = value as {
    hashes?: unknown;
    schemas?: unknown;
  };

  const hasValidHashes =
    candidate.hashes === undefined ||
    (Array.isArray(candidate.hashes) && candidate.hashes.every((hash) => typeof hash === "string"));
  const hasValidSchemas =
    candidate.schemas === undefined ||
    (Array.isArray(candidate.schemas) &&
      candidate.schemas.every((schema) => {
        if (typeof schema !== "object" || schema === null) {
          return false;
        }
        const entry = schema as { hash?: unknown; publishedAt?: unknown };
        return (
          typeof entry.hash === "string" &&
          (entry.publishedAt === undefined ||
            entry.publishedAt === null ||
            (typeof entry.publishedAt === "number" && Number.isFinite(entry.publishedAt)))
        );
      }));

  return hasValidHashes && hasValidSchemas;
}

export async function fetchSchemaHashes(
  serverUrl: string,
  options: FetchStoredSchemasOptions,
): Promise<{ hashes: string[]; schemas: StoredSchemaHash[] }> {
  const rawSchemaHashesResponse = await fetchAdminJson(
    appScopedUrl(serverUrl, options.appId, "schemas"),
    options.adminSecret,
    "Schema hashes",
    // A 404 also deliberately hides invalid/mismatched app IDs. It is not a
    // transient-readiness signal, so leave the retry decision with the caller.
    (status) =>
      status === 404
        ? " Check the server URL and app ID. If this Cloud app was just created, wait for its regional status to become healthy and synced, then retry deployment. A 404 alone does not confirm startup is still in progress."
        : "",
  );
  const schemaHashesResponse = isSchemaHashesResponseBody(rawSchemaHashesResponse)
    ? rawSchemaHashesResponse
    : {};
  const schemas =
    schemaHashesResponse.schemas?.map((schema) => ({
      hash: schema.hash,
      publishedAt: publishedAtEpochMilliseconds(schema.publishedAt),
    })) ?? [];

  return {
    hashes: schemaHashesResponse.hashes ?? schemas.map((schema) => schema.hash),
    schemas,
  };
}

export interface FetchPublishedBundlesOptions {
  appId: string;
  adminSecret: string;
}

/** A permissions bundle the core has published: what sessions pin to. */
export interface PublishedBundle {
  bundle: string;
  schemaHash: string;
  publishedAt: number;
  permissions: Record<string, TablePolicies>;
}

/** Every bundle the core has published, oldest first. */
export async function fetchPublishedBundles(
  serverUrl: string,
  options: FetchPublishedBundlesOptions,
): Promise<PublishedBundle[]> {
  const body = (await fetchAdminJson(
    appScopedUrl(serverUrl, options.appId, "admin/bundles"),
    options.adminSecret,
    "Bundles",
  )) as { bundles?: PublishedBundle[] };
  return body.bundles ?? [];
}

export type PublishedMigrationValue =
  | { type: "Integer"; value: number }
  | { type: "BigInt"; value: bigint | string | number }
  | { type: "Double"; value: number }
  | { type: "Boolean"; value: boolean }
  | { type: "Text"; value: string }
  | { type: "Timestamp"; value: number }
  | { type: "Uuid"; value: string }
  | { type: "Bytea"; value: number[] }
  | { type: "Array"; value: PublishedMigrationValue[] }
  | { type: "Row"; value: { id?: string; values: PublishedMigrationValue[] } }
  | { type: "Enum"; value: { case: string; values: PublishedMigrationValue[] } }
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

export function encodePublishedMigrationValue(value: WasmValue): PublishedMigrationValue {
  switch (value.type) {
    case "BigInt":
      return {
        type: "BigInt",
        value: value.value.toString(),
      };
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
    case "Enum":
      return {
        type: "Enum",
        value: {
          case: value.value.case,
          values: value.value.values.map(encodePublishedMigrationValue),
        },
      };
    default:
      return value;
  }
}

export interface DeploymentArtifacts {
  targetSchemaHash: string;
  schemas: Array<{ hash: string; schema: WasmSchema }>;
  migrations: Array<{ fromHash: string; toHash: string; forward: PublishedTableLens[] }>;
  permissions: CompiledPermissionsMap;
}

export interface DeploymentRequest extends Omit<DeploymentArtifacts, "schemas"> {
  schemas: Array<{ hash: string; schema: { tables: WasmSchema } }>;
}

export interface DeploymentResponse {
  changed: boolean;
  published: {
    schemas: string[];
    migrations: Array<{ fromHash: string; toHash: string }>;
  };
  /** The published bundle's id: what a backend pins its sessions to. */
  bundle: string;
}

export class DeploymentError extends Error {
  readonly name = "DeploymentError";
  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
    readonly details?: { target?: string; active?: string; roots?: string[]; tables?: string[] },
  ) {
    super(message);
  }
}

export async function publishDeployment(
  serverUrl: string,
  options: {
    appId: string;
    adminSecret: string;
    allowDroppingHistory?: boolean;
  } & DeploymentArtifacts,
): Promise<DeploymentResponse> {
  // A core that predates bundles has no `/bundles` route, so it refuses the
  // publish instead of activating it.
  const deployUrl = appScopedUrl(serverUrl, options.appId, "admin/bundles");
  const response = await fetchFor(deployUrl)(deployUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Admin-Secret": options.adminSecret,
    },
    body: JSON.stringify(
      {
        targetSchemaHash: options.targetSchemaHash,
        schemas: options.schemas.map(({ hash, schema }) => ({ hash, schema: { tables: schema } })),
        migrations: options.migrations,
        permissions: normalizePermissionsForWasm(options.permissions),
        ...(options.allowDroppingHistory ? { allowDroppingHistory: true } : {}),
      },
      runtimeSchemaJsonReplacer,
    ),
  });
  if (!response.ok) {
    const text = await response.text();
    let body: { error?: string; code?: string; details?: DeploymentError["details"] } = {};
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === "object") body = parsed;
    } catch {
      /* Preserve non-JSON HTTP errors. */
    }
    throw new DeploymentError(
      `Deploy failed: ${body.error ?? `${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`}`,
      response.status,
      body.code,
      body.details,
    );
  }
  return (await response.json()) as DeploymentResponse;
}
