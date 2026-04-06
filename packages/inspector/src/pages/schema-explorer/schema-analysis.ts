import type { ColumnDescriptor, TableSchema, WasmSchema } from "jazz-tools";
import type { StoredMigrationEdge } from "jazz-tools";

export type SchemaDiffState = "unchanged" | "added" | "removed" | "changed" | "unknown";

export interface GhostEdge {
  fromHash: string;
  toHash: string;
  score: number;
}

export interface UnknownColumnMapping {
  fromColumn: string;
  toColumn: string;
}

export interface ComparedColumn {
  key: string;
  name: string;
  state: SchemaDiffState;
  left?: ColumnDescriptor;
  right?: ColumnDescriptor;
}

export interface ComparedTable {
  tableName: string;
  state: Exclude<SchemaDiffState, "unknown">;
  columns: ComparedColumn[];
  unknownColumnMappings: UnknownColumnMapping[];
  left?: TableSchema;
  right?: TableSchema;
}

export interface ComparedReference {
  id: string;
  fromTable: string;
  fromColumn: string;
  toTable: string;
  state: Exclude<SchemaDiffState, "unknown">;
}

export interface SchemaComparison {
  hasCompatibilityPath: boolean;
  tables: ComparedTable[];
  references: ComparedReference[];
}

export interface SchemaStats {
  tableCount: number;
  columnCount: number;
  referenceCount: number;
}

interface ComponentEdgeCandidate extends GhostEdge {
  leftComponent: number;
  rightComponent: number;
}

export function shortHash(hash: string): string {
  return hash.slice(0, 12);
}

export function getSchemaStats(schema: WasmSchema): SchemaStats {
  const tables = Object.values(schema);
  return {
    tableCount: tables.length,
    columnCount: tables.reduce((count, table) => count + table.columns.length, 0),
    referenceCount: tables.reduce(
      (count, table) => count + table.columns.filter((column) => column.references).length,
      0,
    ),
  };
}

export function hasCompatibilityPath(
  fromHash: string,
  toHash: string,
  migrations: readonly StoredMigrationEdge[],
): boolean {
  if (fromHash === toHash) {
    return true;
  }

  const adjacency = buildUndirectedAdjacency(migrations);
  const queue = [fromHash];
  const visited = new Set(queue);

  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const next of adjacency.get(current) ?? []) {
      if (next === toHash) {
        return true;
      }
      if (!visited.has(next)) {
        visited.add(next);
        queue.push(next);
      }
    }
  }

  return false;
}

export function findShortestGhostEdges({
  schemas,
  migrations,
}: {
  schemas: Record<string, WasmSchema>;
  migrations: readonly StoredMigrationEdge[];
}): GhostEdge[] {
  const hashes = Object.keys(schemas).sort();
  if (hashes.length < 2) {
    return [];
  }

  const components = connectedComponents(hashes, migrations);
  if (components.length < 2) {
    return [];
  }

  const candidates: ComponentEdgeCandidate[] = [];

  for (let leftComponent = 0; leftComponent < components.length; leftComponent += 1) {
    for (
      let rightComponent = leftComponent + 1;
      rightComponent < components.length;
      rightComponent += 1
    ) {
      let best: ComponentEdgeCandidate | null = null;

      for (const fromHash of components[leftComponent]!) {
        for (const toHash of components[rightComponent]!) {
          const score = structuralDistance(schemas[fromHash]!, schemas[toHash]!);
          const candidate: ComponentEdgeCandidate = {
            fromHash,
            toHash,
            score,
            leftComponent,
            rightComponent,
          };

          if (!best || compareGhostCandidates(candidate, best) < 0) {
            best = candidate;
          }
        }
      }

      if (best) {
        candidates.push(best);
      }
    }
  }

  candidates.sort(compareGhostCandidates);

  const parent = components.map((_, index) => index);
  const selected: GhostEdge[] = [];

  for (const candidate of candidates) {
    const leftRoot = findRoot(parent, candidate.leftComponent);
    const rightRoot = findRoot(parent, candidate.rightComponent);

    if (leftRoot === rightRoot) {
      continue;
    }

    parent[rightRoot] = leftRoot;
    selected.push({
      fromHash: candidate.fromHash,
      toHash: candidate.toHash,
      score: candidate.score,
    });
  }

  return selected;
}

export function compareSchemas(
  left: WasmSchema,
  right: WasmSchema,
  options: { hasCompatibilityPath: boolean },
): SchemaComparison {
  const tableNames = Array.from(new Set([...Object.keys(left), ...Object.keys(right)])).sort();
  const tables: ComparedTable[] = [];

  for (const tableName of tableNames) {
    const leftTable = left[tableName];
    const rightTable = right[tableName];

    if (!leftTable && rightTable) {
      tables.push({
        tableName,
        state: "added",
        columns: rightTable.columns.map((column) => ({
          key: `${tableName}:added:${column.name}`,
          name: column.name,
          state: "added",
          right: column,
        })),
        unknownColumnMappings: [],
        right: rightTable,
      });
      continue;
    }

    if (leftTable && !rightTable) {
      tables.push({
        tableName,
        state: "removed",
        columns: leftTable.columns.map((column) => ({
          key: `${tableName}:removed:${column.name}`,
          name: column.name,
          state: "removed",
          left: column,
        })),
        unknownColumnMappings: [],
        left: leftTable,
      });
      continue;
    }

    if (!leftTable || !rightTable) {
      continue;
    }

    const columns: ComparedColumn[] = [];
    const unknownColumnMappings: UnknownColumnMapping[] = [];
    const matchedRightColumns = new Set<string>();

    for (const leftColumn of leftTable.columns) {
      const rightColumn = rightTable.columns.find((column) => column.name === leftColumn.name);

      if (!rightColumn) {
        continue;
      }

      matchedRightColumns.add(rightColumn.name);
      columns.push({
        key: `${tableName}:matched:${leftColumn.name}`,
        name: leftColumn.name,
        state: sameColumnShape(leftColumn, rightColumn) ? "unchanged" : "changed",
        left: leftColumn,
        right: rightColumn,
      });
    }

    const unmatchedLeftColumns = leftTable.columns.filter(
      (column) => !columns.some((entry) => entry.left?.name === column.name),
    );
    const unmatchedRightColumns = rightTable.columns.filter(
      (column) => !matchedRightColumns.has(column.name),
    );
    const consumedRightColumns = new Set<string>();

    if (!options.hasCompatibilityPath) {
      for (const leftColumn of unmatchedLeftColumns) {
        const match = unmatchedRightColumns.find(
          (rightColumn) =>
            !consumedRightColumns.has(rightColumn.name) &&
            unknownMappingCompatible(leftColumn, rightColumn),
        );

        if (!match) {
          continue;
        }

        consumedRightColumns.add(match.name);
        unknownColumnMappings.push({
          fromColumn: leftColumn.name,
          toColumn: match.name,
        });
        columns.push({
          key: `${tableName}:unknown:${leftColumn.name}:${match.name}`,
          name: `${leftColumn.name} -> ${match.name}`,
          state: "unknown",
          left: leftColumn,
          right: match,
        });
      }
    }

    for (const leftColumn of unmatchedLeftColumns) {
      if (unknownColumnMappings.some((mapping) => mapping.fromColumn === leftColumn.name)) {
        continue;
      }

      columns.push({
        key: `${tableName}:removed:${leftColumn.name}`,
        name: leftColumn.name,
        state: "removed",
        left: leftColumn,
      });
    }

    for (const rightColumn of unmatchedRightColumns) {
      if (consumedRightColumns.has(rightColumn.name)) {
        continue;
      }

      columns.push({
        key: `${tableName}:added:${rightColumn.name}`,
        name: rightColumn.name,
        state: "added",
        right: rightColumn,
      });
    }

    const hasChanges = columns.some((column) => column.state !== "unchanged");
    tables.push({
      tableName,
      state: hasChanges ? "changed" : "unchanged",
      columns,
      unknownColumnMappings,
      left: leftTable,
      right: rightTable,
    });
  }

  return {
    hasCompatibilityPath: options.hasCompatibilityPath,
    tables,
    references: compareReferences(left, right),
  };
}

function compareReferences(left: WasmSchema, right: WasmSchema): ComparedReference[] {
  const leftRefs = new Map(collectReferences(left).map((reference) => [reference.id, reference]));
  const rightRefs = new Map(collectReferences(right).map((reference) => [reference.id, reference]));
  const ids = Array.from(new Set([...leftRefs.keys(), ...rightRefs.keys()])).sort();

  return ids.map((id) => {
    const leftRef = leftRefs.get(id);
    const rightRef = rightRefs.get(id);

    if (leftRef && rightRef) {
      return { ...leftRef, state: "unchanged" as const };
    }
    if (leftRef) {
      return { ...leftRef, state: "removed" as const };
    }
    return { ...rightRefs.get(id)!, state: "added" as const };
  });
}

function collectReferences(schema: WasmSchema): Omit<ComparedReference, "state">[] {
  const references: Omit<ComparedReference, "state">[] = [];

  for (const [tableName, table] of Object.entries(schema)) {
    for (const column of table.columns) {
      if (!column.references) {
        continue;
      }

      references.push({
        id: `${tableName}:${column.name}:${column.references}`,
        fromTable: tableName,
        fromColumn: column.name,
        toTable: column.references,
      });
    }
  }

  return references.sort((left, right) => left.id.localeCompare(right.id));
}

function connectedComponents(
  hashes: readonly string[],
  migrations: readonly StoredMigrationEdge[],
): string[][] {
  const adjacency = buildUndirectedAdjacency(migrations);
  const visited = new Set<string>();
  const components: string[][] = [];

  for (const hash of hashes) {
    if (visited.has(hash)) {
      continue;
    }

    const queue = [hash];
    const component: string[] = [];
    visited.add(hash);

    while (queue.length > 0) {
      const current = queue.shift()!;
      component.push(current);

      for (const next of adjacency.get(current) ?? []) {
        if (visited.has(next)) {
          continue;
        }

        visited.add(next);
        queue.push(next);
      }
    }

    component.sort();
    components.push(component);
  }

  return components;
}

function buildUndirectedAdjacency(
  migrations: readonly StoredMigrationEdge[],
): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();

  for (const migration of migrations) {
    if (!adjacency.has(migration.fromHash)) {
      adjacency.set(migration.fromHash, new Set());
    }
    if (!adjacency.has(migration.toHash)) {
      adjacency.set(migration.toHash, new Set());
    }

    adjacency.get(migration.fromHash)!.add(migration.toHash);
    adjacency.get(migration.toHash)!.add(migration.fromHash);
  }

  return adjacency;
}

function structuralDistance(left: WasmSchema, right: WasmSchema): number {
  const leftTokens = flattenSchemaTokens(left);
  const rightTokens = flattenSchemaTokens(right);
  const tokenDifference = symmetricDifferenceSize(leftTokens, rightTokens);
  const tableNames = symmetricDifferenceSize(
    new Set(Object.keys(left)),
    new Set(Object.keys(right)),
  );
  const leftStats = getSchemaStats(left);
  const rightStats = getSchemaStats(right);

  return (
    tokenDifference * 10 + tableNames * 3 + Math.abs(leftStats.columnCount - rightStats.columnCount)
  );
}

function flattenSchemaTokens(schema: WasmSchema): Set<string> {
  const tokens = new Set<string>();

  for (const table of Object.values(schema)) {
    for (const column of table.columns) {
      tokens.add(columnShape(column));
    }
  }

  return tokens;
}

function symmetricDifferenceSize(left: Set<string>, right: Set<string>): number {
  let difference = 0;

  for (const value of left) {
    if (!right.has(value)) {
      difference += 1;
    }
  }

  for (const value of right) {
    if (!left.has(value)) {
      difference += 1;
    }
  }

  return difference;
}

function sameColumnShape(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return columnShape(left) === columnShape(right);
}

function unknownMappingCompatible(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return comparableColumnShape(left) === comparableColumnShape(right);
}

function columnShape(column: ColumnDescriptor): string {
  return [
    column.name,
    serializeColumnType(column.column_type),
    column.nullable ? "nullable" : "required",
    column.references ?? "",
  ].join(":");
}

function comparableColumnShape(column: ColumnDescriptor): string {
  return [
    serializeColumnType(column.column_type),
    column.nullable ? "nullable" : "required",
    column.references ?? "",
  ].join(":");
}

function serializeColumnType(columnType: ColumnDescriptor["column_type"]): string {
  switch (columnType.type) {
    case "Array":
      return `Array(${serializeColumnType(columnType.element as ColumnDescriptor["column_type"])})`;
    case "Enum":
      return `Enum(${columnType.variants.join("|")})`;
    case "Json":
      return "Json";
    case "Row":
      return `Row(${columnType.columns
        .map((column) => `${column.name}:${serializeColumnType(column.column_type)}`)
        .join(",")})`;
    default:
      return columnType.type;
  }
}

function compareGhostCandidates(
  left: ComponentEdgeCandidate,
  right: ComponentEdgeCandidate,
): number {
  return (
    left.score - right.score ||
    left.fromHash.localeCompare(right.fromHash) ||
    left.toHash.localeCompare(right.toHash)
  );
}

function findRoot(parent: number[], index: number): number {
  if (parent[index] === index) {
    return index;
  }

  parent[index] = findRoot(parent, parent[index]!);
  return parent[index]!;
}
