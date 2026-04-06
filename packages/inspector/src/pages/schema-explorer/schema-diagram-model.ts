import type { ColumnDescriptor, WasmSchema } from "jazz-tools";
import type { ComparedReference, SchemaComparison, SchemaDiffState } from "./schema-analysis.js";
import type { SchemaTableNodeView, SchemaTableReferenceView } from "./SchemaCanvas.js";

export function buildSingleSchemaDiagram(schema: WasmSchema, subtitle?: string) {
  const nodes: SchemaTableNodeView[] = Object.entries(schema)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([tableName, table]) => ({
      id: tableName,
      tableName,
      subtitle,
      state: "unchanged",
      columns: table.columns.map((column) => ({
        name: column.name,
        typeLabel: formatColumnType(column),
        nullable: column.nullable,
        reference: column.references,
        state: "unchanged" as SchemaDiffState,
      })),
    }));

  return {
    nodes,
    edges: collectSchemaReferences(schema),
  };
}

export function buildComparisonDiagram(comparison: SchemaComparison) {
  const nodes: SchemaTableNodeView[] = comparison.tables
    .filter((table) => table.state !== "unchanged")
    .map((table) => ({
      id: table.tableName,
      tableName: table.tableName,
      subtitle: table.state === "changed" ? "changed" : table.state,
      state: table.state,
      unknownMappings: table.unknownColumnMappings,
      columns: table.columns.map((column) => ({
        name: column.name,
        typeLabel: formatComparedColumnType(column.left, column.right),
        nullable: column.right?.nullable ?? column.left?.nullable ?? false,
        reference: column.right?.references ?? column.left?.references,
        state: column.state,
      })),
    }));

  const edges: SchemaTableReferenceView[] = comparison.references
    .filter((reference) => reference.state !== "unchanged")
    .map((reference) => comparisonReferenceToEdge(reference));

  return { nodes, edges };
}

function collectSchemaReferences(schema: WasmSchema): SchemaTableReferenceView[] {
  const references: SchemaTableReferenceView[] = [];

  for (const [tableName, table] of Object.entries(schema)) {
    for (const column of table.columns) {
      if (!column.references) {
        continue;
      }

      references.push({
        id: `${tableName}:${column.name}:${column.references}`,
        sourceTable: tableName,
        sourceColumn: column.name,
        targetTable: column.references,
        label: column.name,
        state: "unchanged",
      });
    }
  }

  return references.sort((left, right) => left.id.localeCompare(right.id));
}

function comparisonReferenceToEdge(reference: ComparedReference): SchemaTableReferenceView {
  return {
    id: reference.id,
    sourceTable: reference.fromTable,
    sourceColumn: reference.fromColumn,
    targetTable: reference.toTable,
    label: reference.fromColumn,
    state: reference.state,
  };
}

function formatComparedColumnType(left?: ColumnDescriptor, right?: ColumnDescriptor): string {
  if (left && right) {
    const leftLabel = formatColumnType(left);
    const rightLabel = formatColumnType(right);
    return leftLabel === rightLabel ? leftLabel : `${leftLabel} -> ${rightLabel}`;
  }

  return formatColumnType(right ?? left!);
}

function formatColumnType(column: ColumnDescriptor): string {
  const base = serializeColumnType(column.column_type);
  if (column.references) {
    return `${base} ref`;
  }
  return base;
}

function serializeColumnType(columnType: ColumnDescriptor["column_type"]): string {
  switch (columnType.type) {
    case "Array":
      return `${serializeColumnType(columnType.element as ColumnDescriptor["column_type"])}[]`;
    case "Enum":
      return `enum(${columnType.variants.join(",")})`;
    case "Json":
      return "json";
    case "Row":
      return "row";
    default:
      return columnType.type.toLowerCase();
  }
}
