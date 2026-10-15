// Schema/lens checks shared by runtime ingestion and deployment preparation.
import {
  columnSchemaEquals,
  columnTypeEquals,
  nodeError,
  physicalValueEpochIsCompatible,
  validateRegisteredTransform,
  valueEquals,
} from "../../_extern/node-policy-peer.js";

const invalid = (msg: string) => nodeError("InvalidCatalogueUpdate", msg);

export function validateMigrationLensBetween(lens: any, source: any, target: any): void {
  for (const tableLens of lens.tableLenses) {
    const sourceTable = source.schema.tables.find((t: any) => t.name === tableLens.sourceTable);
    if (!sourceTable) throw invalid("table lens is unknown");
    const targetTable = target.schema.tables.find((t: any) => t.name === tableLens.targetTable);
    if (!targetTable) throw invalid("table lens is unknown");
    const targetBindings = new Set<string>(targetTable.branchBy);
    const branchColumns = new Set<string>(sourceTable.branchBy);
    const columns = new Map<string, any>(sourceTable.columns.map((c: any) => [c.name, { ...c }]));
    let sawTableRename = sourceTable.name === targetTable.name;
    for (const op of tableLens.ops) {
      switch (op.kind) {
        case "RenameTable":
          if (sawTableRename || op.from !== sourceTable.name || op.to !== targetTable.name)
            throw invalid("table rename does not match lens endpoints");
          sawTableRename = true;
          break;
        case "RenameColumn": {
          if (columns.has(op.to)) throw invalid("column rename collides with existing column");
          const column = columns.get(op.from);
          if (!column) throw invalid("column rename source is unknown");
          columns.delete(op.from);
          columns.set(op.to, { ...column, name: op.to });
          if (branchColumns.delete(op.from)) branchColumns.add(op.to);
          break;
        }
        case "CopyColumn": {
          if (columns.has(op.to)) throw invalid("column copy collides with existing column");
          const column = columns.get(op.from);
          if (!column) throw invalid("column copy source is unknown");
          columns.set(op.to, { ...column, name: op.to });
          break;
        }
        case "AddColumn": {
          if (columns.has(op.column)) throw invalid("added column already exists");
          const targetColumn = targetTable.columns.find((c: any) => c.name === op.column);
          if (!targetColumn) throw invalid("added column is absent from target");
          columns.set(op.column, { ...targetColumn });
          if (targetBindings.has(op.column) && targetColumn.default === undefined)
            throw invalid("added branch column requires a migration default");
          break;
        }
        case "DropColumn":
          if (branchColumns.has(op.column)) throw invalid("table branch columns cannot be removed");
          if (!columns.delete(op.column)) throw invalid("dropped column is absent from source");
          break;
        case "TransformColumn": {
          if (branchColumns.has(op.column))
            throw invalid("branch column type and migration default are immutable");
          validateRegisteredTransform(op.transform);
          const sourceColumn = columns.get(op.column);
          if (!sourceColumn) throw invalid("transform column is unknown");
          const targetColumn = targetTable.columns.find((c: any) => c.name === op.column);
          if (!targetColumn) throw invalid("transformed column is absent from target");
          if (
            !physicalValueEpochIsCompatible(sourceColumn.columnType, targetColumn.columnType) ||
            sourceColumn.largeValueKind !== targetColumn.largeValueKind
          )
            throw invalid("column transform changes physical value or large-value semantic kind");
          columns.set(op.column, { ...targetColumn });
          break;
        }
        case "RejectSourceDelta":
          break;
      }
    }
    if (!sawTableRename) throw invalid("renamed table requires an explicit RenameTable operation");
    for (const c of branchColumns)
      if (!targetBindings.has(c)) throw invalid("table branch columns cannot be removed");
    const targetColumns = new Map<string, any>(
      targetTable.columns.map((c: any) => [c.name, { ...c }]),
    );
    for (const branchColumn of branchColumns) {
      const s = columns.get(branchColumn);
      const t = targetColumns.get(branchColumn);
      if (!s || !t) continue;
      if (!columnTypeEquals(s.columnType, t.columnType) || !valueEquals(s.default, t.default))
        throw invalid("branch column type and migration default are immutable");
    }
    for (const c of columns.values()) c.default = undefined;
    for (const c of targetColumns.values()) c.default = undefined;
    const same =
      columns.size === targetColumns.size &&
      [...columns].every(
        ([name, c]) => targetColumns.has(name) && columnSchemaEquals(c, targetColumns.get(name)),
      );
    if (!same) throw invalid("lens operations do not reproduce target columns");
  }
}

export function validateLineageTablePartition(
  source: any,
  target: any,
  lens: any,
  newTables: string[],
  droppedTables: string[],
): void {
  const sourceTables = new Set<string>(source.tables.map((t: any) => t.name));
  const targetTables = new Set<string>(target.tables.map((t: any) => t.name));
  const relatedSource = new Set<string>(lens.tableLenses.map((t: any) => t.sourceTable));
  const relatedTarget = new Set<string>(lens.tableLenses.map((t: any) => t.targetTable));
  const added = new Set(newTables);
  const dropped = new Set(droppedTables);
  const disjoint = (a: Set<string>, b: Set<string>) => [...a].every((x) => !b.has(x));
  const unionEquals = (a: Set<string>, b: Set<string>, expect: Set<string>) => {
    const u = new Set([...a, ...b]);
    return u.size === expect.size && [...u].every((x) => expect.has(x));
  };
  if (
    relatedSource.size !== lens.tableLenses.length ||
    relatedTarget.size !== lens.tableLenses.length ||
    added.size !== newTables.length ||
    dropped.size !== droppedTables.length ||
    !disjoint(relatedSource, dropped) ||
    !disjoint(relatedTarget, added) ||
    !unionEquals(relatedSource, dropped, sourceTables) ||
    !unionEquals(relatedTarget, added, targetTables)
  )
    throw invalid("lineage table declarations do not partition schemas");
}
