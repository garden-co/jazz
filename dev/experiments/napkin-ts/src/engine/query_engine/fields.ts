import type { ClaimPath } from "./input.js";
import {
  APP_COLUMN_PREFIX,
  appStorageColumnName,
  type DescriptorField,
  type TableSchema,
} from "../../_extern/engine.js";

export const USER_COLUMN_PREFIX = APP_COLUMN_PREFIX;
export const AGGREGATE_OUTPUT_PREFIX = "__jazz_aggregate_";
export const LEFT_JOIN_PREFIX = "left.";
export const RIGHT_JOIN_PREFIX = "right.";
export const CLOSURE_REQUIRED_ELEMENT = "__closure_required_element";

const ROUTE_PARAM_PREFIX = "__jazz_route_";
const CLAIM_PARAM_PREFIX = "__jazz_claim_";

export const userColumnField = (column: string): string => appStorageColumnName(column);

export function currentRowFieldNames(table: TableSchema): string[] {
  return [
    "row_uuid",
    ...table.columns.map((column: any) => userColumnField(column.name)),
    "$createdBy",
    "$createdAt",
    "$updatedBy",
    "$updatedAt",
    "tx_time",
    "tx_node_id",
  ];
}

export const aggregateOutputField = (output: string): string => aggregateOutputColumn(output);

export const aggregateOutputAppField = (output: string): string =>
  userColumnField(aggregateOutputField(output));

export function aggregateOutputColumn(output: string): string {
  return output.startsWith(AGGREGATE_OUTPUT_PREFIX)
    ? output
    : `${AGGREGATE_OUTPUT_PREFIX}${output}`;
}

export function aggregateOutputLogicalName(column: string): string | undefined {
  return column.startsWith(AGGREGATE_OUTPUT_PREFIX)
    ? column.slice(AGGREGATE_OUTPUT_PREFIX.length)
    : undefined;
}

const ENGINE_FIELDS = new Set([
  "$createdBy",
  "$createdAt",
  "$updatedBy",
  "$updatedAt",
  "created_by",
  "created_at",
  "updated_by",
  "updated_at",
  "branch_key",
  "row_uuid",
  "tx_time",
  "tx_node_id",
  "schema_version",
  "authored_columns",
  "global_time",
  "settle_position",
]);

export function descriptorPublicName(field: DescriptorField): string | undefined {
  const name: string | undefined = field.logicalName();
  if (name === undefined) return undefined;
  if (field.name !== name) return name;
  if (ENGINE_FIELDS.has(name)) return undefined;
  return aggregateOutputLogicalName(name) ?? (!name.startsWith("__jazz_") ? name : undefined);
}

export const joinField = (prefix: string, field: string): string => `${prefix}${field}`;
export const leftField = (field: string): string => joinField(LEFT_JOIN_PREFIX, field);
export const rightField = (field: string): string => joinField(RIGHT_JOIN_PREFIX, field);
export const routeParamField = (param: string): string => `${ROUTE_PARAM_PREFIX}${param}`;

export function routeParamFromField(field: string): string | undefined {
  return field.startsWith(ROUTE_PARAM_PREFIX) ? field.slice(ROUTE_PARAM_PREFIX.length) : undefined;
}

export function claimParamField(path: ClaimPath): string {
  if (path.length === 1 && !path[0].includes("_") && !path[0].includes(":")) {
    return `${CLAIM_PARAM_PREFIX}${path[0]}`;
  }
  let field = `${CLAIM_PARAM_PREFIX}v1:`;
  for (const segment of path) field += `${segment.length}:${segment}`;
  return field;
}

function parseLen(s: string): number | undefined {
  return /^\d+$/.test(s) ? Number(s) : undefined;
}

export function claimPathFromParamField(field: string): ClaimPath | undefined {
  if (!field.startsWith(CLAIM_PARAM_PREFIX)) return undefined;
  let rest = field.slice(CLAIM_PARAM_PREFIX.length);
  if (rest.startsWith("typed:")) {
    const typed = rest.slice(6);
    const colon = typed.indexOf(":");
    if (colon < 0) return undefined;
    const len = parseLen(typed.slice(0, colon));
    const tail = typed.slice(colon + 1);
    if (len === undefined || tail.length <= len || tail[len] !== ":") return undefined;
    return claimPathFromParamField(tail.slice(len + 1));
  }
  if (!rest.startsWith("v1:")) return rest.split("_");
  rest = rest.slice(3);
  const segments: string[] = [];
  while (rest.length > 0) {
    const colon = rest.indexOf(":");
    if (colon < 0) return undefined;
    const len = parseLen(rest.slice(0, colon));
    const tail = rest.slice(colon + 1);
    if (len === undefined || tail.length < len) return undefined;
    segments.push(tail.slice(0, len));
    rest = tail.slice(len);
  }
  return segments;
}

export const tableUserColumnField = (table: string, column: string): string =>
  `${USER_COLUMN_PREFIX}_${table}__${column}`;
