import { spawn, type ChildProcess } from "node:child_process";
import { createHmac, randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { definePermissions } from "../permissions/index.js";
import { translateQuery } from "./query-adapter.js";
import { sendSyncPayload } from "./sync-transport.js";
import { hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";
import type { WasmSchema } from "../drivers/types.js";

type AppContext = import("./context.js").AppContext;
type JazzClient = import("./client.js").JazzClient;
type Row = import("./client.js").Row;

const INTERNAL_API_SECRET = "jazz-ts-internal-api-secret";
const SECRET_HASH_KEY = "jazz-ts-secret-hash-key";
const ADMIN_SECRET = "jazz-ts-admin-secret";
const BACKEND_SECRET = "jazz-ts-backend-secret";
const JWT_KID = "jazz-ts-kid";
const JWT_SECRET = "jazz-ts-jwt-secret";
const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

type SocialPolicyStyle = "split" | "join" | "hopToWhere";

interface SocialProfile {
  id: string;
  displayName: string;
  principalId: string;
}

interface SocialProfileWhere {
  id?: string;
  displayName?: string;
  principalId?: string;
}

interface SocialPerson {
  id: string;
  profileId: string;
  principalId: string;
}

interface SocialPersonWhere {
  id?: string;
  profileId?: string;
  principalId?: string;
}

interface SocialFriendship {
  id: string;
  personAId: string;
  personBId: string;
  personAPrincipal: string;
  personBPrincipal: string;
}

interface SocialFriendshipWhere {
  id?: string;
  personAId?: string;
  personBId?: string;
  personAPrincipal?: string;
  personBPrincipal?: string;
}

class SocialProfileQueryBuilder {
  declare readonly _rowType: SocialProfile;
  where(_input: SocialProfileWhere): SocialProfileQueryBuilder {
    return this;
  }
}

class SocialPersonQueryBuilder {
  declare readonly _rowType: SocialPerson;
  where(_input: SocialPersonWhere): SocialPersonQueryBuilder {
    return this;
  }
}

class SocialFriendshipQueryBuilder {
  declare readonly _rowType: SocialFriendship;
  where(_input: SocialFriendshipWhere): SocialFriendshipQueryBuilder {
    return this;
  }
}

type SocialSeed = {
  alicePrincipal: string;
  bobProfileId: string;
  carolPrincipal: string;
  carolProfileId: string;
  eveProfileId: string;
};

type CloudServerConfig = {
  dataRoot: string;
};

type CloudServerHandle = {
  child: ChildProcess;
  port: number;
  baseUrl: string;
};

type CreatedApp = {
  app_id: string;
};

const tempDirsToCleanup: string[] = [];

function allocTempDir(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  tempDirsToCleanup.push(dir);
  return dir;
}

function base64url(input: Buffer | string): string {
  const encoded = (input instanceof Buffer ? input : Buffer.from(input)).toString("base64");
  return encoded.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

function signJwt(sub: string, secret: string): string {
  const header = {
    alg: "HS256",
    typ: "JWT",
    kid: JWT_KID,
  };
  const payload = {
    sub,
    iss: "https://issuer.jazz.ts.test",
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const signedPart = `${headerB64}.${payloadB64}`;
  const sig = createHmac("sha256", secret).update(signedPart).digest();
  return `${signedPart}.${base64url(sig)}`;
}

function makeSyncPayload() {
  return {
    ObjectUpdated: {
      object_id: randomUUID(),
      metadata: null,
      branch_name: "main",
      commits: [],
    },
  };
}

function resolveCargoTargetDir(): string {
  const runtimeDir = dirname(fileURLToPath(import.meta.url));
  const repoRoot = join(runtimeDir, "../../../../");
  const configuredTargetDir = process.env.CARGO_TARGET_DIR;
  if (!configuredTargetDir) {
    return join(repoRoot, "target");
  }
  return isAbsolute(configuredTargetDir)
    ? configuredTargetDir
    : join(repoRoot, configuredTargetDir);
}

function findCloudServerBinary(): string | null {
  const targetDir = resolveCargoTargetDir();
  const candidates = [
    join(targetDir, "debug", "jazz-cloud-server"),
    join(targetDir, "release", "jazz-cloud-server"),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) return candidate;
    if (existsSync(`${candidate}.exe`)) return `${candidate}.exe`;
  }
  return null;
}

function assertIntegrationPrerequisites(): void {
  const hasWasm = hasJazzWasmBuild();
  const targetDir = resolveCargoTargetDir();
  const binaryPath = findCloudServerBinary();
  if (hasWasm && binaryPath) return;

  const missing: string[] = [];
  if (!hasWasm) {
    missing.push("missing Jazz WASM runtime artifacts");
  }
  if (!binaryPath) {
    missing.push(
      `missing jazz-cloud-server binary under ${targetDir}/{debug,release}/jazz-cloud-server`,
    );
  }

  throw new Error(
    [
      "Cloud-server TS integration prerequisites are missing:",
      ...missing.map((entry) => `- ${entry}`),
      "Build prerequisites, then rerun tests:",
      "1. pnpm --filter @jazz/rust build:crates",
    ].join("\n"),
  );
}

function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("failed to allocate free port"));
        return;
      }
      const port = address.port;
      server.close((err) => {
        if (err) reject(err);
        else resolve(port);
      });
    });
    server.on("error", reject);
  });
}

async function waitForHealth(baseUrl: string): Promise<void> {
  const healthUrl = `${baseUrl}/health`;
  for (let i = 0; i < 100; i++) {
    try {
      const response = await fetch(healthUrl);
      if (response.ok) return;
    } catch {
      // Not ready yet.
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`cloud-server failed health check at ${healthUrl}`);
}

async function startCloudServer(config: CloudServerConfig): Promise<CloudServerHandle> {
  const binary = findCloudServerBinary();
  if (!binary) {
    throw new Error("jazz-cloud-server binary not found");
  }

  const port = await getFreePort();
  const child = spawn(
    binary,
    [
      "--port",
      String(port),
      "--data-root",
      config.dataRoot,
      "--internal-api-secret",
      INTERNAL_API_SECRET,
      "--secret-hash-key",
      SECRET_HASH_KEY,
      "--worker-threads",
      "1",
    ],
    {
      stdio: ["ignore", "pipe", "pipe"],
      env: process.env,
    },
  );

  const baseUrl = `http://127.0.0.1:${port}`;
  await waitForHealth(baseUrl);
  return { child, port, baseUrl };
}

async function stopProcess(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.killed) return;
  child.kill("SIGTERM");
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
      }
      resolve();
    }, 2000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

async function createApp(baseUrl: string, jwksEndpoint: string): Promise<CreatedApp> {
  const response = await fetch(`${baseUrl}/internal/apps`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Jazz-Internal-Secret": INTERNAL_API_SECRET,
    },
    body: JSON.stringify({
      app_name: "jazz-ts-cloud-server-test",
      jwks_endpoint: jwksEndpoint,
      backend_secret: BACKEND_SECRET,
      admin_secret: ADMIN_SECRET,
    }),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`create app failed ${response.status}: ${text}`);
  }

  return (await response.json()) as CreatedApp;
}

async function waitForRows(
  client: JazzClient,
  queryJson: string,
  predicate: (rows: Row[]) => boolean,
  timeoutMs = 20000,
  tier: "edge" | undefined = "edge",
): Promise<Row[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: Row[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await client.query(queryJson, tier ? { tier } : undefined);
      if (predicate(rows)) return rows;
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }
    await new Promise((r) => setTimeout(r, 150));
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `timed out waiting for predicate; lastRows=${JSON.stringify(lastRows)}, lastError=${lastErrorMessage}`,
  );
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

async function connectClient(context: AppContext): Promise<JazzClient> {
  const [clientMod, runtimeUtils] = await Promise.all([
    import("./client.js"),
    import("./testing/wasm-runtime-test-utils.js"),
  ]);

  const runtime = await runtimeUtils.createWasmRuntime(context.schema, {
    appId: context.appId,
    env: context.env,
    userBranch: context.userBranch,
    tier: "worker",
  });

  return clientMod.JazzClient.connectWithRuntime(runtime, context);
}

class JwksServer {
  private server: Server;
  readonly url: string;

  private constructor(server: Server, url: string) {
    this.server = server;
    this.url = url;
  }

  static async start(secret: string): Promise<JwksServer> {
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      if (req.url !== "/jwks") {
        res.statusCode = 404;
        res.end("not found");
        return;
      }

      const body = JSON.stringify({
        keys: [
          {
            kty: "oct",
            kid: JWT_KID,
            alg: "HS256",
            k: base64url(secret),
          },
        ],
      });
      res.statusCode = 200;
      res.setHeader("Content-Type", "application/json");
      res.end(body);
    });

    const port = await getFreePort();
    await new Promise<void>((resolve, reject) => {
      server.listen(port, "127.0.0.1", (err?: unknown) => {
        if (err) reject(err);
        else resolve();
      });
    });

    return new JwksServer(server, `http://127.0.0.1:${port}/jwks`);
  }

  async stop(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

function makeSocialBaseSchema(): WasmSchema {
  return {
    profiles: {
      columns: [
        { name: "displayName", column_type: { type: "Text" }, nullable: false },
        { name: "principalId", column_type: { type: "Text" }, nullable: false },
      ],
    },
    people: {
      columns: [
        {
          name: "profileId",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "profiles",
        },
        {
          name: "principalId",
          column_type: { type: "Text" },
          nullable: false,
        },
      ],
    },
    friendships: {
      columns: [
        {
          name: "personAId",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "people",
        },
        {
          name: "personBId",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "people",
        },
        {
          name: "personAPrincipal",
          column_type: { type: "Text" },
          nullable: false,
        },
        {
          name: "personBPrincipal",
          column_type: { type: "Text" },
          nullable: false,
        },
      ],
    },
  };
}

function asRecord(value: unknown, context: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Expected object for ${context}`);
  }
  return value as Record<string, unknown>;
}

function normalizeWasmLiteral(value: unknown): unknown {
  if (value === null) return { type: "Null" };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number") {
    if (!Number.isInteger(value) || !Number.isFinite(value)) {
      throw new Error("relation literal numbers must be finite integers");
    }
    if (value >= -2147483648 && value <= 2147483647) return { type: "Integer", value };
    return { type: "BigInt", value };
  }
  if (typeof value === "string") {
    const uuidLike =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
    return uuidLike ? { type: "Uuid", value } : { type: "Text", value };
  }
  if (Array.isArray(value)) {
    return { type: "Array", value: value.map((entry) => normalizeWasmLiteral(entry)) };
  }

  const object = asRecord(value, "literal");
  // Already in internally-tagged Value enum form.
  if (typeof object.type === "string") return object;
  // Accept legacy externally-tagged test literals and normalize them.
  if (Object.keys(object).length === 1) {
    const [legacyType, legacyValue] = Object.entries(object)[0]!;
    if (legacyType === "Null") return { type: "Null" };
    return { type: legacyType, value: legacyValue };
  }
  throw new Error("relation literal object must use typed Value enum representation");
}

function toSerdeColumnRef(input: unknown): unknown {
  const record = asRecord(input, "column ref");
  return {
    scope: typeof record.scope === "string" ? record.scope : null,
    column: record.column,
  };
}

function toSerdeValueRef(input: unknown): unknown {
  const record = asRecord(input, "value ref");
  const type = record.type;
  if (type === "Literal") return { Literal: normalizeWasmLiteral(record.value) };
  if (type === "SessionRef") return { SessionRef: record.path };
  if (type === "OuterColumn") return { OuterColumn: toSerdeColumnRef(record.column) };
  if (type === "FrontierColumn") return { FrontierColumn: toSerdeColumnRef(record.column) };
  if (type === "RowId") return { RowId: record.source };
  throw new Error(`Unsupported value ref type in ExistsRel conversion: ${String(type)}`);
}

function toSerdePredicate(input: unknown): unknown {
  const record = asRecord(input, "predicate");
  const type = record.type;
  if (type === "Cmp") {
    return {
      Cmp: {
        left: toSerdeColumnRef(record.left),
        op: record.op,
        right: toSerdeValueRef(record.right),
      },
    };
  }
  if (type === "Contains") {
    return {
      Contains: {
        left: toSerdeColumnRef(record.left),
        right: toSerdeValueRef(record.value),
      },
    };
  }
  if (type === "IsNull") return { IsNull: { column: toSerdeColumnRef(record.column) } };
  if (type === "IsNotNull") return { IsNotNull: { column: toSerdeColumnRef(record.column) } };
  if (type === "In") {
    const values = Array.isArray(record.values) ? record.values : [];
    return {
      In: {
        left: toSerdeColumnRef(record.left),
        values: values.map((value) => toSerdeValueRef(value)),
      },
    };
  }
  if (type === "And") {
    const exprs = Array.isArray(record.exprs) ? record.exprs : [];
    return { And: exprs.map((expr) => toSerdePredicate(expr)) };
  }
  if (type === "Or") {
    const exprs = Array.isArray(record.exprs) ? record.exprs : [];
    return { Or: exprs.map((expr) => toSerdePredicate(expr)) };
  }
  if (type === "Not") return { Not: toSerdePredicate(record.expr) };
  if (type === "True") return "True";
  if (type === "False") return "False";
  throw new Error(`Unsupported predicate type in ExistsRel conversion: ${String(type)}`);
}

function toSerdeKeyRef(input: unknown): unknown {
  const record = asRecord(input, "key ref");
  const type = record.type;
  if (type === "Column") return { Column: toSerdeColumnRef(record.column) };
  if (type === "RowId") return { RowId: record.source };
  throw new Error(`Unsupported key ref type in ExistsRel conversion: ${String(type)}`);
}

function toSerdeProjectExpr(input: unknown): unknown {
  const record = asRecord(input, "project expr");
  const type = record.type;
  if (type === "Column") return { Column: toSerdeColumnRef(record.column) };
  if (type === "RowId") return { RowId: record.source };
  throw new Error(`Unsupported project expr type in ExistsRel conversion: ${String(type)}`);
}

function toSerdeRelExpr(input: unknown): unknown {
  const record = asRecord(input, "relation expr");
  const type = record.type;
  if (typeof type !== "string") {
    return record;
  }
  if (type === "TableScan") return { TableScan: { table: record.table } };
  if (type === "Filter") {
    return {
      Filter: {
        input: toSerdeRelExpr(record.input),
        predicate: toSerdePredicate(record.predicate),
      },
    };
  }
  if (type === "Join") {
    const on = Array.isArray(record.on) ? record.on : [];
    return {
      Join: {
        left: toSerdeRelExpr(record.left),
        right: toSerdeRelExpr(record.right),
        on: on.map((entry) => {
          const condition = asRecord(entry, "join condition");
          return {
            left: toSerdeColumnRef(condition.left),
            right: toSerdeColumnRef(condition.right),
          };
        }),
        join_kind: record.joinKind,
      },
    };
  }
  if (type === "Project") {
    const columns = Array.isArray(record.columns) ? record.columns : [];
    return {
      Project: {
        input: toSerdeRelExpr(record.input),
        columns: columns.map((entry) => {
          const column = asRecord(entry, "project column");
          return {
            alias: column.alias,
            expr: toSerdeProjectExpr(column.expr),
          };
        }),
      },
    };
  }
  if (type === "Gather") {
    const dedupeKey = Array.isArray(record.dedupeKey) ? record.dedupeKey : [];
    return {
      Gather: {
        seed: toSerdeRelExpr(record.seed),
        step: toSerdeRelExpr(record.step),
        frontier_key: toSerdeKeyRef(record.frontierKey),
        max_depth: record.maxDepth,
        dedupe_key: dedupeKey.map((entry) => toSerdeKeyRef(entry)),
      },
    };
  }
  if (type === "Distinct") {
    const key = Array.isArray(record.key) ? record.key : [];
    return {
      Distinct: {
        input: toSerdeRelExpr(record.input),
        key: key.map((entry) => toSerdeKeyRef(entry)),
      },
    };
  }
  if (type === "OrderBy") {
    const terms = Array.isArray(record.terms) ? record.terms : [];
    return {
      OrderBy: {
        input: toSerdeRelExpr(record.input),
        terms: terms.map((entry) => {
          const term = asRecord(entry, "order term");
          return {
            column: toSerdeColumnRef(term.column),
            direction: term.direction,
          };
        }),
      },
    };
  }
  if (type === "Offset") {
    return {
      Offset: {
        input: toSerdeRelExpr(record.input),
        offset: record.offset,
      },
    };
  }
  if (type === "Limit") {
    return {
      Limit: {
        input: toSerdeRelExpr(record.input),
        limit: record.limit,
      },
    };
  }
  throw new Error(`Unsupported relation type in ExistsRel conversion: ${String(type)}`);
}

function toSerdePolicyValue(input: unknown): unknown {
  const record = asRecord(input, "policy value");
  const type = record.type;
  if (type === "Literal") return { type: "Literal", value: normalizeWasmLiteral(record.value) };
  if (type === "SessionRef") return { type: "SessionRef", path: record.path };
  return record;
}

function normalizePolicyExprForWasm(input: unknown): unknown {
  const expr = asRecord(input, "policy expr");
  const type = expr.type;
  if (typeof type !== "string") return expr;
  if (type === "Cmp") {
    return {
      type: "Cmp",
      column: expr.column,
      op: expr.op,
      value: toSerdePolicyValue(expr.value),
    };
  }
  if (type === "IsNull") return { type: "IsNull", column: expr.column };
  if (type === "IsNotNull") return { type: "IsNotNull", column: expr.column };
  if (type === "Contains") {
    return {
      type: "Contains",
      column: expr.column,
      value: toSerdePolicyValue(expr.value),
    };
  }
  if (type === "In") {
    return {
      type: "In",
      column: expr.column,
      session_path: expr.session_path,
    };
  }
  if (type === "InList") {
    const values = Array.isArray(expr.values) ? expr.values : [];
    return {
      type: "InList",
      column: expr.column,
      values: values.map((value) => toSerdePolicyValue(value)),
    };
  }
  if (type === "Exists") {
    return {
      type: "Exists",
      table: expr.table,
      condition: normalizePolicyExprForWasm(expr.condition),
    };
  }
  if (type === "ExistsRel") {
    return {
      type: "ExistsRel",
      rel: toSerdeRelExpr(expr.rel),
    };
  }
  if (type === "Inherits") {
    return {
      type: "Inherits",
      operation: expr.operation,
      via_column: expr.via_column,
      max_depth: expr.max_depth,
    };
  }
  if (type === "InheritsReferencing") {
    return {
      type: "InheritsReferencing",
      operation: expr.operation,
      source_table: expr.source_table,
      via_column: expr.via_column,
      max_depth: expr.max_depth,
    };
  }
  if (type === "And") {
    const exprs = Array.isArray(expr.exprs) ? expr.exprs : [];
    return { type: "And", exprs: exprs.map((entry) => normalizePolicyExprForWasm(entry)) };
  }
  if (type === "Or") {
    const exprs = Array.isArray(expr.exprs) ? expr.exprs : [];
    return { type: "Or", exprs: exprs.map((entry) => normalizePolicyExprForWasm(entry)) };
  }
  if (type === "Not") {
    return { type: "Not", expr: normalizePolicyExprForWasm(expr.expr) };
  }
  if (type === "True") return { type: "True" };
  if (type === "False") return { type: "False" };
  throw new Error(`Unsupported policy expr type in schema normalization: ${type}`);
}

function normalizePermissionsForWasm<T>(permissions: T): T {
  const out: Record<string, unknown> = {};
  for (const [tableName, tablePolicies] of Object.entries(permissions as Record<string, unknown>)) {
    const policiesRecord = asRecord(tablePolicies, `policies for ${tableName}`);
    const normalizedTable: Record<string, unknown> = {
      select: {},
      insert: {},
      update: {},
      delete: {},
    };
    const operations = ["select", "insert", "update", "delete"] as const;
    for (const operation of operations) {
      const opPolicy = policiesRecord[operation];
      if (!opPolicy) continue;
      const opPolicyRecord = asRecord(opPolicy, `${tableName}.${operation}`);
      const normalizedOperation: Record<string, unknown> = {};
      if (opPolicyRecord.using) {
        normalizedOperation.using = normalizePolicyExprForWasm(opPolicyRecord.using);
      }
      if (opPolicyRecord.with_check) {
        normalizedOperation.with_check = normalizePolicyExprForWasm(opPolicyRecord.with_check);
      }
      normalizedTable[operation] = normalizedOperation;
    }
    out[tableName] = normalizedTable;
  }
  return out as T;
}

function buildSocialSchema(style: SocialPolicyStyle): WasmSchema {
  const schema = makeSocialBaseSchema();
  const socialApp = {
    profiles: new SocialProfileQueryBuilder(),
    people: new SocialPersonQueryBuilder(),
    friendships: new SocialFriendshipQueryBuilder(),
    wasmSchema: schema,
  };

  const permissions = normalizePermissionsForWasm(
    definePermissions(socialApp, ({ policy, anyOf, allowedTo, session }) => {
      const sessionPersonId = session.user_id;

      if (style === "split") {
        policy.people.allowRead.where((person) =>
          anyOf([
            policy.exists(
              policy.friendships.where({
                personAPrincipal: sessionPersonId,
                personBPrincipal: person.principalId,
              }),
            ),
            policy.exists(
              policy.friendships.where({
                personBPrincipal: sessionPersonId,
                personAPrincipal: person.principalId,
              }),
            ),
          ]),
        );
        policy.profiles.allowRead.where(allowedTo.readReferencing(policy.people, "profileId"));
        return;
      }

      if (style === "join") {
        policy.profiles.allowRead.where((profile) =>
          anyOf([
            policy.exists(
              policy.people
                .where({ principalId: profile.principalId })
                .join(policy.friendships, { left: "id", right: "personAId" })
                .where({ personBPrincipal: sessionPersonId }),
            ),
            policy.exists(
              policy.people
                .where({ principalId: profile.principalId })
                .join(policy.friendships, { left: "id", right: "personBId" })
                .where({ personAPrincipal: sessionPersonId }),
            ),
          ]),
        );
        return;
      }

      policy.profiles.allowRead.where((profile) =>
        anyOf([
          policy.exists(
            policy.people
              .where({ principalId: profile.principalId })
              .hopTo("friendshipsViaPersonAId")
              .where({ personBPrincipal: sessionPersonId }),
          ),
          policy.exists(
            policy.people
              .where({ principalId: profile.principalId })
              .hopTo("friendshipsViaPersonBId")
              .where({ personAPrincipal: sessionPersonId }),
          ),
        ]),
      );
    }),
  );

  const tables: WasmSchema = {};
  for (const [tableName, tableSchema] of Object.entries(schema)) {
    const tablePolicies = permissions[tableName];
    tables[tableName] = tablePolicies
      ? ({
          ...tableSchema,
          policies: tablePolicies as unknown as (typeof tableSchema)["policies"],
        } as (typeof tables)[string])
      : tableSchema;
  }
  return tables;
}

function buildAllRowsQuery(schema: WasmSchema, table: string): string {
  return translateQuery(
    JSON.stringify({
      table,
      conditions: [],
      includes: {},
      orderBy: [],
      offset: 0,
    }),
    schema,
  );
}

function buildProfileByIdQuery(schema: WasmSchema, id: string): string {
  return translateQuery(
    JSON.stringify({
      table: "profiles",
      conditions: [{ column: "id", op: "eq", value: id }],
      includes: {},
      orderBy: [],
      offset: 0,
    }),
    schema,
  );
}

async function seedSocialGraph(client: JazzClient): Promise<SocialSeed> {
  const aliceProfileId = await client.create(
    "profiles",
    [
      { type: "Text", value: "alice" },
      { type: "Text", value: "alice" },
    ],
    { tier: "edge" },
  );
  const bobProfileId = await client.create(
    "profiles",
    [
      { type: "Text", value: "bob" },
      { type: "Text", value: "bob" },
    ],
    { tier: "edge" },
  );
  const carolProfileId = await client.create(
    "profiles",
    [
      { type: "Text", value: "carol" },
      { type: "Text", value: "carol" },
    ],
    { tier: "edge" },
  );
  const eveProfileId = await client.create(
    "profiles",
    [
      { type: "Text", value: "eve" },
      { type: "Text", value: "eve" },
    ],
    { tier: "edge" },
  );
  const alicePrincipal = "alice";
  const bobPrincipal = "bob";
  const carolPrincipal = "carol";
  const evePrincipal = "eve";

  const alicePersonId = await client.create(
    "people",
    [
      { type: "Uuid", value: aliceProfileId },
      { type: "Text", value: alicePrincipal },
    ],
    { tier: "edge" },
  );
  const bobPersonId = await client.create(
    "people",
    [
      { type: "Uuid", value: bobProfileId },
      { type: "Text", value: bobPrincipal },
    ],
    { tier: "edge" },
  );
  await client.create(
    "people",
    [
      { type: "Uuid", value: carolProfileId },
      { type: "Text", value: carolPrincipal },
    ],
    { tier: "edge" },
  );
  const evePersonId = await client.create(
    "people",
    [
      { type: "Uuid", value: eveProfileId },
      { type: "Text", value: evePrincipal },
    ],
    { tier: "edge" },
  );

  await client.create(
    "friendships",
    [
      { type: "Uuid", value: alicePersonId },
      { type: "Uuid", value: bobPersonId },
      { type: "Text", value: alicePrincipal },
      { type: "Text", value: bobPrincipal },
    ],
    { tier: "edge" },
  );
  await client.create(
    "friendships",
    [
      { type: "Uuid", value: evePersonId },
      { type: "Uuid", value: alicePersonId },
      { type: "Text", value: evePrincipal },
      { type: "Text", value: alicePrincipal },
    ],
    { tier: "edge" },
  );

  return {
    alicePrincipal,
    bobProfileId,
    carolPrincipal,
    carolProfileId,
    eveProfileId,
  };
}

async function runSocialReadPermissionsScenario(style: SocialPolicyStyle): Promise<void> {
  const socialSchema = buildSocialSchema(style);
  const queryAllProfiles = buildAllRowsQuery(socialSchema, "profiles");
  const queryAllFriendships = buildAllRowsQuery(socialSchema, "friendships");

  const jwks = await JwksServer.start(JWT_SECRET);
  const dataRoot = allocTempDir(`jazz-ts-cloud-server-social-${style}-`);
  const server = await startCloudServer({ dataRoot });
  let seeder: JazzClient | null = null;

  try {
    const app = await createApp(server.baseUrl, jwks.url);

    seeder = await connectClient(
      makeContext(app.app_id, server.baseUrl, signJwt("seed-user", JWT_SECRET), socialSchema),
    );
    const seeded = await seedSocialGraph(seeder);

    const aliceSession = seeder.forSession({ user_id: seeded.alicePrincipal, claims: {} });
    const carolSession = seeder.forSession({ user_id: seeded.carolPrincipal, claims: {} });

    const aliceProfiles = await withTimeout(
      aliceSession.query(queryAllProfiles),
      10000,
      "alice session query(all profiles) timed out",
    );
    const visibleIds = [...new Set(aliceProfiles.map((row) => row.id))].sort();
    expect(visibleIds).toEqual([seeded.bobProfileId, seeded.eveProfileId].sort());

    const bobRows = await withTimeout(
      aliceSession.query(buildProfileByIdQuery(socialSchema, seeded.bobProfileId)),
      10000,
      "alice session query(bob profile) timed out",
    );
    expect(bobRows).toHaveLength(1);
    expect(bobRows[0]?.values[0]).toEqual({ type: "Text", value: "bob" });

    const carolRowsForAlice = await withTimeout(
      aliceSession.query(buildProfileByIdQuery(socialSchema, seeded.carolProfileId)),
      10000,
      "alice session query(carol profile) timed out",
    );
    expect(carolRowsForAlice).toEqual([]);

    const carolFriendships = await withTimeout(
      carolSession.query(queryAllFriendships),
      10000,
      "carol session query(friendships) timed out",
    );
    expect(carolFriendships).toHaveLength(2);
    const carolProfiles = await withTimeout(
      carolSession.query(queryAllProfiles),
      10000,
      "carol session query(all profiles) timed out",
    );
    expect(carolProfiles).toEqual([]);
  } finally {
    if (seeder) await seeder.shutdown();
    await stopProcess(server.child);
    await jwks.stop();
  }
}

function makeContext(
  appId: string,
  serverUrl: string,
  jwtToken: string,
  schema: WasmSchema = TEST_SCHEMA,
): AppContext {
  return {
    appId,
    schema,
    serverUrl,
    serverPathPrefix: `/apps/${appId}`,
    env: "test",
    userBranch: "main",
    jwtToken,
    adminSecret: ADMIN_SECRET,
    backendSecret: BACKEND_SECRET,
  };
}

afterEach(() => {
  while (tempDirsToCleanup.length > 0) {
    const dir = tempDirsToCleanup.pop()!;
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // best effort cleanup
    }
  }
});

describe("cloud-server integration (Jazz TS)", () => {
  beforeAll(() => {
    assertIntegrationPrerequisites();
  });

  it("routes sync requests through serverPathPrefix with JWT auth", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-cloud-server-");
    const server = await startCloudServer({ dataRoot });

    try {
      const app = await createApp(server.baseUrl, jwks.url);
      const pathPrefix = `/apps/${app.app_id}`;

      await sendSyncPayload(
        server.baseUrl,
        JSON.stringify(makeSyncPayload()),
        false,
        { jwtToken: signJwt("valid-user", JWT_SECRET), pathPrefix },
        "[valid] ",
      );

      await expect(
        sendSyncPayload(
          server.baseUrl,
          JSON.stringify(makeSyncPayload()),
          false,
          { jwtToken: signJwt("invalid-user", "wrong-secret"), pathPrefix },
          "[invalid] ",
        ),
      ).rejects.toThrow("401");
    } finally {
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("links local anonymous identity to external JWT via JazzClient call path", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-cloud-server-link-");
    const server = await startCloudServer({ dataRoot });
    let client: JazzClient | null = null;

    try {
      const app = await createApp(server.baseUrl, jwks.url);
      client = await connectClient({
        ...makeContext(app.app_id, server.baseUrl, signJwt("linked-user", JWT_SECRET)),
        localAuthMode: "anonymous",
        localAuthToken: "device-token-a",
      });

      const first = await client.linkExternalIdentity();
      expect(first.created).toBe(true);
      expect(first.subject).toBe("linked-user");

      const second = await client.linkExternalIdentity();
      expect(second.created).toBe(false);
      expect(second.principal_id).toBe(first.principal_id);
    } finally {
      if (client) await client.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("resolves empty settled-tier query snapshots", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-cloud-server-empty-query-");
    const server = await startCloudServer({ dataRoot });
    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    let client: JazzClient | null = null;
    try {
      const app = await createApp(server.baseUrl, jwks.url);
      client = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("empty-snapshot", JWT_SECRET)),
      );

      const rows = await waitForRows(
        client,
        queryAllTodos,
        (all) => all.length === 0,
        20000,
        "edge",
      );
      expect(rows).toEqual([]);
    } finally {
      if (client) await client.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("syncs queries and mutations between two TS clients via cloud-server", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-cloud-server-");
    const server = await startCloudServer({ dataRoot });

    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    let clientA: JazzClient | null = null;
    let clientB: JazzClient | null = null;

    try {
      const app = await createApp(server.baseUrl, jwks.url);
      clientA = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("a", JWT_SECRET)),
      );
      clientB = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("b", JWT_SECRET)),
      );

      const rowId = await clientA.create(
        "todos",
        [
          { type: "Text", value: "shared-item" },
          { type: "Boolean", value: false },
        ],
        { tier: "edge" },
      );

      const rowsAfterCreate = await waitForRows(clientB, queryAllTodos, (rows) =>
        rows.some((row) => row.id === rowId),
      );
      const createdRow = rowsAfterCreate.find((row) => row.id === rowId);
      expect(createdRow?.values[0]).toEqual({ type: "Text", value: "shared-item" });

      await clientA.update(rowId, { done: { type: "Boolean", value: true } }, { tier: "edge" });
      const rowsAfterUpdate = await waitForRows(clientB, queryAllTodos, (rows) => {
        const row = rows.find((r) => r.id === rowId);
        return Boolean(row && row.values[1]?.type === "Boolean" && row.values[1].value === true);
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.values[1]).toEqual({ type: "Boolean", value: true });

      await clientA.delete(rowId, { tier: "edge" });
      await waitForRows(clientB, queryAllTodos, (rows) => !rows.some((row) => row.id === rowId));
    } finally {
      if (clientA) await clientA.shutdown();
      if (clientB) await clientB.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 30000);

  it("enforces split social read permissions (exists + readReferencing)", async () => {
    await runSocialReadPermissionsScenario("split");
  }, 60000);

  it("enforces social read permissions with one-clause join(...)", async () => {
    await runSocialReadPermissionsScenario("join");
  }, 60000);

  it("enforces social read permissions with one-clause hopTo(...).where(...)", async () => {
    await runSocialReadPermissionsScenario("hopToWhere");
  }, 60000);

  it("resyncs data from cloud-server after server restart", async () => {
    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-cloud-server-restart-");
    const queryAllTodos = translateQuery(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        offset: 0,
      }),
      TEST_SCHEMA,
    );

    const appId = await (async () => {
      const server = await startCloudServer({ dataRoot });
      let writer: JazzClient | null = null;
      try {
        const app = await createApp(server.baseUrl, jwks.url);
        writer = await connectClient(
          makeContext(app.app_id, server.baseUrl, signJwt("writer", JWT_SECRET)),
        );
        await writer.create(
          "todos",
          [
            { type: "Text", value: "persisted-item" },
            { type: "Boolean", value: false },
          ],
          { tier: "edge" },
        );
        await waitForRows(writer, queryAllTodos, (rows) => rows.length >= 1, 15000);
        return app.app_id;
      } finally {
        if (writer) await writer.shutdown();
        await stopProcess(server.child);
      }
    })();

    const restarted = await startCloudServer({ dataRoot });
    let reader: JazzClient | null = null;
    try {
      reader = await connectClient(
        makeContext(appId, restarted.baseUrl, signJwt("reader", JWT_SECRET)),
      );
      const rows = await waitForRows(reader, queryAllTodos, (all) => all.length >= 1, 20000);
      expect(
        rows.some(
          (row) => row.values[0]?.type === "Text" && row.values[0].value === "persisted-item",
        ),
      ).toBe(true);
    } finally {
      if (reader) await reader.shutdown();
      await stopProcess(restarted.child);
      await jwks.stop();
    }
  }, 90000);
});

// ---------------------------------------------------------------------------
// Policy bypass reproduction: subscription without session skips filtering
// ---------------------------------------------------------------------------

interface OwnedItem {
  id: string;
  title: string;
  ownerId: string;
}

interface OwnedItemWhere {
  id?: string;
  title?: string;
  ownerId?: string;
}

class OwnedItemQueryBuilder {
  declare readonly _rowType: OwnedItem;
  where(_input: OwnedItemWhere): OwnedItemQueryBuilder {
    return this;
  }
}

function buildOwnedItemsSchema(): WasmSchema {
  const schema: WasmSchema = {
    owned_items: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "ownerId", column_type: { type: "Text" }, nullable: false },
      ],
    },
  };

  const app = {
    owned_items: new OwnedItemQueryBuilder(),
    wasmSchema: schema,
  };

  const permissions = normalizePermissionsForWasm(
    definePermissions(app, ({ policy, session }) => {
      policy.owned_items.allowRead.where({ ownerId: session.user_id });
    }),
  );

  const tables: WasmSchema = {};
  for (const [tableName, tableSchema] of Object.entries(schema)) {
    const tablePolicies = permissions[tableName];
    tables[tableName] = tablePolicies
      ? ({
          ...tableSchema,
          policies: tablePolicies as unknown as (typeof tableSchema)["policies"],
        } as (typeof tables)[string])
      : tableSchema;
  }
  return tables;
}

describe("Policy bypass: subscription without session skips PolicyFilterNode", () => {
  beforeAll(() => {
    assertIntegrationPrerequisites();
  });

  it("query() and subscribe() should filter by the JWT session", async () => {
    const schema = buildOwnedItemsSchema();
    const queryAllItems = buildAllRowsQuery(schema, "owned_items");

    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-policy-bypass-");
    const server = await startCloudServer({ dataRoot });
    let seeder: JazzClient | null = null;
    let aliceClient: JazzClient | null = null;

    try {
      const app = await createApp(server.baseUrl, jwks.url);

      // Seed other users' rows via a separate client.
      seeder = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("seed-user", JWT_SECRET), schema),
      );

      await seeder.create(
        "owned_items",
        [
          { type: "Text", value: "bob-item" },
          { type: "Text", value: "bob" },
        ],
        { tier: "edge" },
      );
      await seeder.create(
        "owned_items",
        [
          { type: "Text", value: "carol-item" },
          { type: "Text", value: "carol" },
        ],
        { tier: "edge" },
      );

      await seeder.shutdown();
      seeder = null;

      // Connect as alice and insert her own row.
      aliceClient = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("alice", JWT_SECRET), schema),
      );
      await aliceClient.create(
        "owned_items",
        [
          { type: "Text", value: "alice-item" },
          { type: "Text", value: "alice" },
        ],
        { tier: "edge" },
      );

      // Establish sync by fetching data without session first.
      await aliceClient.queryInternal(queryAllItems, undefined, { tier: "edge" });

      // query() should only return alice's row.
      const queryRows = await waitForRows(aliceClient, queryAllItems, (rows) => rows.length >= 1);
      const queryTitles = queryRows
        .map((row) => (row.values[0] as { type: "Text"; value: string }).value)
        .sort();
      expect(queryTitles).toEqual(["alice-item"]);

      // subscribe() should also only return alice's row.
      const subscribedRows = await new Promise<Row[]>((resolve, reject) => {
        const collected = new Map<string, Row>();
        const timer = setTimeout(() => {
          resolve([...collected.values()]);
        }, 5000);

        aliceClient!.subscribe(queryAllItems, (delta) => {
          for (const change of delta) {
            if (change.kind === 0) {
              collected.set(change.id, { id: change.id, values: change.row.values });
            } else if (change.kind === 1) {
              collected.delete(change.id);
            }
          }
        });

        setTimeout(() => {
          clearTimeout(timer);
          reject(new Error("subscribe timed out after 20s"));
        }, 20000);
      });

      const subscribeTitles = subscribedRows
        .map((row) => (row.values[0] as { type: "Text"; value: string }).value)
        .sort();

      expect(subscribeTitles).toEqual(["alice-item"]);
    } finally {
      if (seeder) await seeder.shutdown();
      if (aliceClient) await aliceClient.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 60000);

  // Server-side defence in depth: even when a query explicitly omits the
  // session, the server should fall back to the connection-level session
  // (hashed principal ID) and apply the PolicyFilterNode. Because the hashed
  // ID won't match ownerId values written with the raw JWT sub claim, the
  // policy filter returns zero rows — fail closed rather than fail open.
  it("server falls back to connection-level session when query omits session (fail closed)", async () => {
    const schema = buildOwnedItemsSchema();
    const queryAllItems = buildAllRowsQuery(schema, "owned_items");

    const jwks = await JwksServer.start(JWT_SECRET);
    const dataRoot = allocTempDir("jazz-ts-policy-server-fallback-");
    const server = await startCloudServer({ dataRoot });
    let aliceClient: JazzClient | null = null;
    let bobClient: JazzClient | null = null;

    try {
      const app = await createApp(server.baseUrl, jwks.url);

      // Alice connects and inserts her own row.
      aliceClient = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("alice", JWT_SECRET), schema),
      );
      await aliceClient.create(
        "owned_items",
        [
          { type: "Text", value: "alice-item" },
          { type: "Text", value: "alice" },
        ],
        { tier: "edge" },
      );

      // Bob connects and queries WITHOUT a session (explicitly undefined).
      // This sends QuerySubscription { session: None } to the server.
      bobClient = await connectClient(
        makeContext(app.app_id, server.baseUrl, signJwt("bob", JWT_SECRET), schema),
      );
      const rows = await bobClient.queryInternal(queryAllItems, undefined, { tier: "edge" });

      // Server should fall back to Bob's connection-level session.
      // The hashed principal ID won't match Alice's ownerId, so zero rows.
      expect(rows).toEqual([]);
    } finally {
      if (aliceClient) await aliceClient.shutdown();
      if (bobClient) await bobClient.shutdown();
      await stopProcess(server.child);
      await jwks.stop();
    }
  }, 60000);
});
