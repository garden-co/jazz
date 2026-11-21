// The generic read endpoint (POST /api/query): a whitelisted, tenant-scoped
// list query, the one shape every BigLabel screen asks for (filter, search,
// sort, page, include a related row). Tables, columns and relations come from
// schema.ts. Every table, and every included row, is filtered to what the
// caller may read (permissions.server.ts). Sign-in emails aren't in schema.ts,
// so no query can reach them.
import { z } from "zod";
import { tables, type Include, type QuerySpec, type TableName } from "../../schema";
import { rows, type Db } from "./db.server";
import { myLabelsSql } from "./permissions.server";

const MAX_LIMIT = 5000; // the Overview's bounded counts

const condition = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
  z.object({ contains: z.string() }),
  z.object({ gte: z.number() }),
  z.object({ isNull: z.boolean() }),
]);
const include: z.ZodType<Include> = z.lazy(() => z.record(z.string(), z.union([z.literal(true), include])));
export const querySpec = z.object({
  table: z.enum(Object.keys(tables) as [TableName, ...TableName[]]),
  where: z.record(z.string(), condition),
  orderBy: z.array(z.tuple([z.string(), z.enum(["asc", "desc"])])),
  include,
  select: z.array(z.string()).optional(),
  limit: z.number().int().min(0).max(MAX_LIMIT).optional(),
  offset: z.number().int().min(0).optional(),
});

const quote = (name: string) => `"${name}"`;

function column(table: TableName, name: string) {
  if (!(tables[table].columns as readonly string[]).includes(name)) throw new Error(`unknown column ${table}.${name}`);
  return quote(name);
}

/** SQL: which rows of `table` (aliased `alias`) the caller ($1) may read. */
function readable(table: TableName, alias: string) {
  if (table === "organizations") return `${alias}.id IN ${myLabelsSql("$1")}`;
  if (table === "people")
    return `(${alias}."userId" = $1 OR ${alias}.id IN (SELECT "personId" FROM memberships WHERE "organizationId" IN ${myLabelsSql("$1")}))`;
  return `${alias}."organizationId" IN ${myLabelsSql("$1")}`;
}

/** SQL for one row as JSON, with its included relations nested. */
function rowJson(table: TableName, alias: string, include: Include, select: string[] | undefined, depth: number): string {
  const columns = select ? ["id", ...select.filter((c) => c !== "id")] : [...tables[table].columns];
  const parts = columns.map((c) => `'${c}', ${alias}.${column(table, c)}`);
  const relations = tables[table].relations as Record<string, readonly [TableName, string]>;
  for (const [name, nested] of Object.entries(include)) {
    const relation = relations[name];
    if (!relation) throw new Error(`unknown relation ${table}.${name}`);
    const [target, foreignKey] = relation;
    const child = `r${depth}_${name}`;
    const json = rowJson(target, child, nested === true ? {} : nested, undefined, depth + 1);
    parts.push(
      `'${name}', (SELECT ${json} FROM ${quote(target)} ${child} WHERE ${child}.id = ${alias}.${column(table, foreignKey)} AND ${readable(target, child)})`,
    );
  }
  return `json_build_object(${parts.join(", ")})`;
}

export async function runQuery(db: Db, me: string, spec: QuerySpec) {
  const params: unknown[] = [me];
  const param = (value: unknown) => `$${params.push(value)}`;
  const where = [readable(spec.table, "t")];
  for (const [name, cond] of Object.entries(spec.where)) {
    const col = `t.${column(spec.table, name)}`;
    if (cond === null) where.push(`${col} IS NULL`);
    else if (typeof cond !== "object") where.push(`${col} = ${param(cond)}`);
    else if ("contains" in cond) where.push(`strpos(${col}, ${param(cond.contains)}) > 0`);
    else if ("gte" in cond) where.push(`${col} >= ${param(cond.gte)}`);
    else where.push(`${col} IS ${cond.isNull ? "" : "NOT "}NULL`);
  }
  const order = spec.orderBy.map(([c, dir]) => `t.${column(spec.table, c)} ${dir === "desc" ? "DESC" : "ASC"}`);
  const sql = `SELECT ${rowJson(spec.table, "t", spec.include, spec.select, 0)} AS row FROM ${quote(spec.table)} t
    WHERE ${where.join(" AND ")}
    ${order.length ? `ORDER BY ${order.join(", ")}` : ""}
    LIMIT ${Math.min(spec.limit ?? MAX_LIMIT, MAX_LIMIT)} OFFSET ${spec.offset ?? 0}`;
  return (await rows<{ row: unknown }>(db, sql, params)).map((r) => r.row);
}
