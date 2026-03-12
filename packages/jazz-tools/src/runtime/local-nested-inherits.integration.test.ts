import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { definePermissions } from "../permissions/index.js";
import { JazzClient, type Row } from "./client.js";
import { translateQuery } from "./query-adapter.js";
import { createWasmRuntime } from "./testing/wasm-runtime-test-utils.js";

const JWT_KID = "local-nested-inherits-test-kid";
const JWT_SECRET = "local-nested-inherits-test-secret";

interface GrantTeam {
  id: string;
  ownerId: string;
  name: string;
}

interface GrantTeamWhere {
  id?: string;
  ownerId?: string;
  name?: string;
}

interface GrantResource {
  id: string;
  name: string;
}

interface GrantResourceWhere {
  id?: string;
  name?: string;
}

interface GrantEdge {
  id: string;
  resource: string;
  team: string;
  grantRole: string;
}

interface GrantEdgeWhere {
  id?: string;
  resource?: string;
  team?: string;
  grantRole?: string;
}

interface GrantBranding {
  id: string;
  resourceId: string;
  name: string;
}

interface GrantBrandingWhere {
  id?: string;
  resourceId?: string;
  name?: string;
}

class GrantTeamQueryBuilder {
  declare readonly _rowType: GrantTeam;
  where(_input: GrantTeamWhere): GrantTeamQueryBuilder {
    return this;
  }
}

class GrantResourceQueryBuilder {
  declare readonly _rowType: GrantResource;
  where(_input: GrantResourceWhere): GrantResourceQueryBuilder {
    return this;
  }
}

class GrantEdgeQueryBuilder {
  declare readonly _rowType: GrantEdge;
  where(_input: GrantEdgeWhere): GrantEdgeQueryBuilder {
    return this;
  }
}

class GrantBrandingQueryBuilder {
  declare readonly _rowType: GrantBranding;
  where(_input: GrantBrandingWhere): GrantBrandingQueryBuilder {
    return this;
  }
}

function base64url(input: string): string {
  return Buffer.from(input)
    .toString("base64")
    .replace(/=/g, "")
    .replace(/\+/g, "-")
    .replace(/\//g, "_");
}

function signJwt(sub: string): string {
  const header = {
    alg: "HS256",
    typ: "JWT",
    kid: JWT_KID,
  };
  const payload = {
    sub,
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const signedPart = `${base64url(JSON.stringify(header))}.${base64url(JSON.stringify(payload))}`;
  const signature = createHmac("sha256", JWT_SECRET).update(signedPart).digest("base64url");
  return `${signedPart}.${signature}`;
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

function buildNestedGrantSchema(): WasmSchema {
  const schema: WasmSchema = {
    teams: {
      columns: [
        { name: "ownerId", column_type: { type: "Text" }, nullable: false },
        { name: "name", column_type: { type: "Text" }, nullable: false },
      ],
    },
    resources: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    resource_access_edges: {
      columns: [
        {
          name: "resource",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "resources",
        },
        { name: "team", column_type: { type: "Uuid" }, nullable: false, references: "teams" },
        { name: "grantRole", column_type: { type: "Text" }, nullable: false },
      ],
    },
    brandings: {
      columns: [
        {
          name: "resourceId",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "resources",
        },
        { name: "name", column_type: { type: "Text" }, nullable: false },
      ],
    },
  };

  const app = {
    teams: new GrantTeamQueryBuilder(),
    resources: new GrantResourceQueryBuilder(),
    resource_access_edges: new GrantEdgeQueryBuilder(),
    brandings: new GrantBrandingQueryBuilder(),
    wasmSchema: schema,
  };

  const permissions = definePermissions(app, ({ policy, allowedTo, session }) => {
    policy.teams.allowRead.where({ ownerId: session.user_id });
    policy.resource_access_edges.allowRead.where(allowedTo.read("team"));
    policy.resources.allowRead.where(
      allowedTo.readReferencing(policy.resource_access_edges, "resource"),
    );
    policy.brandings.allowRead.where(allowedTo.read("resourceId"));
  });

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

async function queryRows(client: JazzClient, query: string): Promise<Row[]> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      client.query(query, { tier: "worker", propagation: "local-only" }),
      new Promise<Row[]>((_, reject) => {
        timeoutId = setTimeout(() => reject(new Error(`query timed out: ${query}`)), 1_500);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

describe("local nested inherits regression", () => {
  it("propagates read access from team ownership through access edge to resource and branding", async () => {
    const appId = "local-nested-inherits-repro";
    const schema = buildNestedGrantSchema();
    const runtime = await createWasmRuntime(schema, {
      appId,
      env: "test",
      userBranch: "main",
      tier: "worker",
    });

    const teamId = runtime.insert("teams", [
      { type: "Text", value: "alice" },
      { type: "Text", value: "Alice team" },
    ]) as string;
    const resourceId = runtime.insert("resources", [
      { type: "Text", value: "Shared resource" },
    ]) as string;
    runtime.insert("brandings", [
      { type: "Uuid", value: resourceId },
      { type: "Text", value: "Shared branding" },
    ]);
    runtime.insert("resource_access_edges", [
      { type: "Uuid", value: resourceId },
      { type: "Uuid", value: teamId },
      { type: "Text", value: "editor" },
    ]);

    const client = JazzClient.connectWithRuntime(runtime, {
      appId,
      schema,
      env: "test",
      userBranch: "main",
      jwtToken: signJwt("alice"),
      tier: "worker",
    });

    const edges = await queryRows(client, buildAllRowsQuery(schema, "resource_access_edges"));
    const resources = await queryRows(client, buildAllRowsQuery(schema, "resources"));
    const brandings = await queryRows(client, buildAllRowsQuery(schema, "brandings"));

    expect(edges).toHaveLength(1);
    expect(resources).toHaveLength(1);
    expect(brandings).toHaveLength(1);
  }, 20_000);
});
