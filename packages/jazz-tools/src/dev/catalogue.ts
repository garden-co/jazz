import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
  type StoredPermissionsHead,
} from "../runtime/schema-fetch.js";

export type CatalogueEvent =
  | { type: "schema-loaded"; schemaFile: string }
  | { type: "schema-published"; hash: string; objectId?: string }
  | { type: "schema-skipped"; hash: string; reason: "already-stored" }
  | { type: "permissions-loaded"; permissionsFile: string }
  | { type: "permissions-published"; schemaHash: string; version?: number }
  | { type: "permissions-skipped"; reason: "missing-permissions-file" }
  | { type: "warning"; message: string };

export interface CatalogueProjectOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  schemaDir: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushSchemaOptions extends CatalogueProjectOptions {
  skipIfStored?: boolean;
}

export interface PushSchemaResult {
  hash: string;
  schemaFile: string;
  status: "published" | "already-stored";
  objectId?: string;
}

export interface PushPermissionsOptions extends CatalogueProjectOptions {
  schemaHash?: string;
}

export interface PushPermissionsResult {
  schemaHash: string;
  permissionsFile: string;
  previousHead: StoredPermissionsHead | null;
  head: StoredPermissionsHead | null;
}

export interface PushSchemaCatalogueOptions extends CatalogueProjectOptions {
  env?: string;
  userBranch?: string;
  enableLogs?: boolean;
}

export interface PushMigrationOptions {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  migrationsDir: string;
  fromHash: string;
  toHash: string;
  onEvent?: (event: CatalogueEvent) => void;
}

export interface PushMigrationResult {
  fromHash: string;
  toHash: string;
  status: "published";
  filePath?: string;
}

export interface DeployOptions extends CatalogueProjectOptions {
  migrationsDir: string;
  noVerify?: boolean;
}

export interface DeployResult {
  schema: PushSchemaResult;
  migration?:
    | PushMigrationResult
    | { status: "already-connected"; fromHash: string; toHash: string };
  permissions?: PushPermissionsResult;
  warnings: string[];
}

function emit(options: { onEvent?: (event: CatalogueEvent) => void }, event: CatalogueEvent): void {
  options.onEvent?.(event);
}

function ensurePermissionsProject(compiled: LoadedSchemaProject): LoadedSchemaProject & {
  permissions: NonNullable<LoadedSchemaProject["permissions"]>;
  permissionsFile: string;
} {
  if (!compiled.permissions || !compiled.permissionsFile) {
    throw new Error(
      "No permissions found for this app. Create a permissions.ts file before using permissions commands.",
    );
  }

  return compiled as LoadedSchemaProject & {
    permissions: NonNullable<LoadedSchemaProject["permissions"]>;
    permissionsFile: string;
  };
}

export async function pushSchema(options: PushSchemaOptions): Promise<PushSchemaResult> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  emit(options, { type: "schema-published", hash: result.hash, objectId: result.objectId });

  return {
    hash: result.hash,
    schemaFile: compiled.schemaFile,
    status: "published",
    objectId: result.objectId,
  };
}

export async function pushPermissions(
  options: PushPermissionsOptions,
): Promise<PushPermissionsResult> {
  const compiled = ensurePermissionsProject(await loadCompiledSchema(options.schemaDir));
  emit(options, { type: "permissions-loaded", permissionsFile: compiled.permissionsFile });

  const schemaHash = options.schemaHash;
  if (!schemaHash) {
    throw new Error(
      "Missing schema hash. Push or resolve the structural schema before pushing permissions.",
    );
  }

  const { head: previousHead } = await fetchPermissionsHead(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
  });

  const { head } = await publishStoredPermissions(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schemaHash,
    permissions: compiled.permissions,
    expectedParentBundleObjectId: previousHead?.bundleObjectId ?? null,
  });

  emit(options, { type: "permissions-published", schemaHash, version: head?.version });

  return {
    schemaHash,
    permissionsFile: compiled.permissionsFile,
    previousHead,
    head,
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  emit(options, { type: "schema-loaded", schemaFile: compiled.schemaFile });

  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  emit(options, { type: "schema-published", hash: result.hash, objectId: result.objectId });

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    const { head: permissionsHead } = await publishStoredPermissions(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schemaHash: result.hash,
      permissions: compiled.permissions,
      expectedParentBundleObjectId: head?.bundleObjectId ?? null,
    });
    emit(options, {
      type: "permissions-published",
      schemaHash: result.hash,
      version: permissionsHead?.version,
    });
  }

  if (options.enableLogs === true) {
    console.log(
      `[jazz-schema-push] published ${result.hash} from ${compiled.schemaFile} to ${options.serverUrl}`,
    );
  }

  return { hash: result.hash };
}

export async function pushMigration(_options: PushMigrationOptions): Promise<PushMigrationResult> {
  throw new Error("pushMigration is not implemented yet.");
}

export async function deploy(_options: DeployOptions): Promise<DeployResult> {
  throw new Error("deploy is not implemented yet.");
}
