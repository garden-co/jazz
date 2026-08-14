import { expect } from "vitest";
import { PostcardReader } from "./native-codec.js";

export function readSchemaSelectPolicyBranches(
  schemaBytes: Uint8Array,
  tableName: string,
): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  return readSchemaPolicyBranches(schemaBytes, tableName, "select");
}

export function readSchemaPolicyBranches(
  schemaBytes: Uint8Array,
  tableName: string,
  operation: "select" | "insert" | "updateUsing" | "updateCheck" | "delete",
): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const policies = {
      select: tableReader.option(readPolicyQueryForTest),
      insert: tableReader.option(readPolicyQueryForTest),
      updateUsing: tableReader.option(readPolicyQueryForTest),
      updateCheck: tableReader.option(readPolicyQueryForTest),
      delete: tableReader.option(readPolicyQueryForTest),
    };
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, policy: policies[operation] };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);

  const policy = tables.find((table) => table.table === tableName)?.policy;
  expect(policy).toBeDefined();
  return policy!;
}

export function readSchemaTableMetadata(
  schemaBytes: Uint8Array,
  tableName: string,
): {
  indexedColumns: string[];
  mergeStrategies: Array<{ column: string; strategy: "Lww" | "Counter" }>;
} {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    tableReader.option(readPolicyQueryForTest);
    const indexedColumns = tableReader.readVec((indexReader) => indexReader.string());
    const mergeStrategyCount = tableReader.u64();
    const mergeStrategies: Array<{ column: string; strategy: "Lww" | "Counter" }> = [];
    for (let index = 0; index < mergeStrategyCount; index += 1) {
      const column = tableReader.string();
      const tag = tableReader.u64();
      const strategy = tag === 0 ? "Lww" : tag === 1 ? "Counter" : null;
      if (strategy == null) {
        throw new Error(`unsupported merge strategy tag ${tag}`);
      }
      mergeStrategies.push({ column, strategy });
    }
    return { table, indexedColumns, mergeStrategies };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  const table = tables.find((entry) => entry.table === tableName);
  expect(table).toBeDefined();
  return {
    indexedColumns: table!.indexedColumns,
    mergeStrategies: table!.mergeStrategies,
  };
}

export function readPolicyQueryForTest(reader: PostcardReader): {
  table: string;
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
  branches: TestPolicyBranch[];
} {
  const table = reader.string();
  const filters = reader.readVec(readPolicyPredicateForTest);
  const joins = reader.readVec(readPolicyJoinForTest);
  reader.option(() => undefined);
  const branches = reader.readVec(readPolicyBranchForTest);
  reader.readVec(skipPolicyReachableForTest);
  reader.readVec(readPolicyInheritsForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { table, filters, joins, branches };
}

export function readSchemaSelectPolicyReachables(
  schemaBytes: Uint8Array,
  tableName: string,
): TestPolicyReachable[] {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const select = tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.option(readPolicyQueryWithReachablesForTest);
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, select };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  return tables.find((table) => table.table === tableName)?.select?.reachables ?? [];
}

export function readSchemaSelectPolicyInherits(
  schemaBytes: Uint8Array,
  tableName: string,
): { inherits: TestPolicyInherits[]; joinCount: number } {
  return readSchemaPolicyInherits(schemaBytes, tableName, "select");
}

export function readSchemaPolicyInherits(
  schemaBytes: Uint8Array,
  tableName: string,
  operation: "select" | "insert" | "updateUsing" | "updateCheck" | "delete",
): { inherits: TestPolicyInherits[]; joinCount: number } {
  const reader = new PostcardReader(schemaBytes);
  const tables = reader.readVec((tableReader) => {
    const table = tableReader.string();
    tableReader.readVec((columnReader) => {
      columnReader.string();
      skipSchemaValueType(columnReader);
      columnReader.option(() => undefined);
      columnReader.option(() => undefined);
      columnReader.option(skipGrooveValue);
    });
    const referenceCount = tableReader.u64();
    for (let index = 0; index < referenceCount; index += 1) {
      tableReader.string();
      tableReader.string();
    }
    const policies = {
      select: tableReader.option(readPolicyQueryWithInheritsForTest),
      insert: tableReader.option(readPolicyQueryWithInheritsForTest),
      updateUsing: tableReader.option(readPolicyQueryWithInheritsForTest),
      updateCheck: tableReader.option(readPolicyQueryWithInheritsForTest),
      delete: tableReader.option(readPolicyQueryWithInheritsForTest),
    };
    tableReader.u64();
    const indexCount = tableReader.u64();
    for (let index = 0; index < indexCount; index += 1) {
      tableReader.string();
      tableReader.readVec((indexReader) => indexReader.string());
    }
    return { table, policy: policies[operation] };
  });
  reader.option(() => undefined);
  reader.option(() => undefined);
  return (
    tables.find((table) => table.table === tableName)?.policy ?? { inherits: [], joinCount: 0 }
  );
}

export function readPolicyQueryWithReachablesForTest(reader: PostcardReader): {
  reachables: TestPolicyReachable[];
} {
  reader.string();
  reader.readVec(readPolicyPredicateForTest);
  reader.readVec(readPolicyJoinForTest);
  reader.option(() => undefined);
  reader.readVec(readPolicyBranchForTest);
  const reachables = reader.readVec(readPolicyReachableForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { reachables };
}

export function readPolicyQueryWithInheritsForTest(reader: PostcardReader): {
  inherits: TestPolicyInherits[];
  joinCount: number;
} {
  reader.string();
  reader.readVec(readPolicyPredicateForTest);
  const joinCount = reader.readVec(readPolicyJoinForTest).length;
  reader.option(() => undefined);
  reader.readVec(readPolicyBranchForTest);
  reader.readVec(skipPolicyReachableForTest);
  const inherits = reader.readVec(readPolicyInheritsForTest);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.option(() => undefined);
  reader.u64();
  return { inherits, joinCount };
}

export function readPolicyBranchForTest(reader: PostcardReader): TestPolicyBranch {
  const filters = reader.readVec(readPolicyPredicateForTest);
  const joins = reader.readVec(readPolicyJoinForTest);
  reader.readVec(skipPolicyReachableForTest);
  reader.readVec(readPolicyInheritsForTest);
  return { filters, joins };
}

export function readPolicyInheritsForTest(reader: PostcardReader): TestPolicyInherits {
  const parentColumn = reader.string();
  reader.u64();
  const maxDepth = reader.option((optionReader) => optionReader.u64());
  return maxDepth === undefined ? { parentColumn } : { parentColumn, maxDepth };
}

export function readPolicyJoinForTest(reader: PostcardReader): TestPolicyJoin {
  const table = reader.string();
  const onColumn = reader.string();
  const targetTag = reader.u64();
  const sourceColumn = reader.option((sourceReader) => sourceReader.string());
  const sourceLookup = reader.option((lookupReader) => ({
    table: lookupReader.string(),
    rowIdSourceColumn: lookupReader.string(),
    valueColumn: lookupReader.string(),
  }));
  reader.readVec((correlationReader) => {
    correlationReader.string();
    correlationReader.string();
  });
  const filters = reader.readVec(readPolicyPredicateForTest);
  const nestedJoins = reader.readVec(readPolicyJoinForTest);
  return { table, onColumn, targetTag, sourceColumn, sourceLookup, filters, nestedJoins };
}

export function skipPolicyReachableForTest(reader: PostcardReader): void {
  readPolicyReachableForTest(reader);
}

export function readPolicyReachableForTest(reader: PostcardReader): TestPolicyReachable {
  const accessTable = reader.string();
  const accessRowColumn = reader.string();
  const accessTeamColumn = reader.string();
  const accessTeamTargetTag = reader.u64();
  readPolicyOperandForTest(reader);
  const accessFilters = reader.readVec(readPolicyPredicateForTest);
  const edgeTable = reader.string();
  const edgeMemberColumn = reader.string();
  const edgeParentColumn = reader.string();
  const edgeFilters = reader.readVec(readPolicyPredicateForTest);
  const boundTag = reader.u64();
  const maxDepth = boundTag === 1 ? reader.u64() : 0;
  const seed = reader.option((seedReader) => ({
    table: seedReader.string(),
    userColumn: seedReader.option((userColumnReader) => userColumnReader.string()),
    userClaim: seedReader.option((userClaimReader) => userClaimReader.string()),
    teamColumn: seedReader.string(),
    filters: seedReader.readVec(readPolicyPredicateForTest),
  }));
  return {
    accessTable,
    accessRowColumn,
    accessTeamColumn,
    accessTeamTargetTag,
    accessFilters,
    edgeTable,
    edgeMemberColumn,
    edgeParentColumn,
    edgeFilters,
    maxDepth,
    seed,
  };
}

export function readPolicyPredicateForTest(reader: PostcardReader): TestPolicyPredicate {
  const tag = reader.u64();
  if (tag === 0 || tag === 1) {
    return { tag, children: reader.readVec(readPolicyPredicateForTest) };
  }
  if (tag === 2) {
    return { tag, child: readPolicyPredicateForTest(reader) };
  }
  if ([3, 4, 6, 7, 8, 9].includes(tag)) {
    expect(reader.u64()).toBe(0);
    const column = reader.string();
    return { tag, column, operand: readPolicyOperandForTest(reader) };
  }
  if (tag === 5) {
    readPolicyOperandForTest(reader);
    reader.readVec(readPolicyOperandForTest);
    return { tag };
  }
  if (tag === 10) {
    readPolicyOperandForTest(reader);
    readPolicyOperandForTest(reader);
    return { tag };
  }
  if (tag === 11) {
    readPolicyOperandForTest(reader);
    return { tag };
  }
  throw new Error(`unsupported policy predicate tag ${tag}`);
}

export function readPolicyOperandForTest(reader: PostcardReader): TestPolicyOperand {
  const tag = reader.u64();
  if (tag === 0) return { tag, column: reader.string() };
  if (tag === 2) return { tag, claim: reader.string() };
  if (tag === 3) {
    const literalTag = reader.u64();
    if (literalTag === 2 || literalTag === 3) {
      return { tag, literalTag, value: reader.u64() };
    }
    if (literalTag === 13 || literalTag === 14) {
      return { tag, literalTag, value: reader.i64() };
    }
    if (literalTag === 4) {
      return { tag, literalTag, value: reader.bytes() };
    }
    if (literalTag === 5) {
      return { tag, literalTag, value: reader.bool() };
    }
    if (literalTag === 6) {
      return { tag, literalTag, value: reader.string() };
    }
    if (literalTag === 8) {
      return { tag, literalTag, value: reader.bytes() };
    }
    if (literalTag === 12) {
      return { tag, literalTag, value: reader.option(readPolicyOperandForTest) };
    }
    throw new Error(`unsupported policy literal tag ${literalTag}`);
  }
  throw new Error(`unsupported policy operand tag ${tag}`);
}

export function skipSchemaValueType(reader: PostcardReader): void {
  const tag = reader.u64();
  if (tag === 11) {
    reader.string();
    reader.readVec((variant) => variant.string());
    return;
  }
  if (tag === 12) {
    reader.readVec(skipSchemaValueType);
    return;
  }
  if (tag === 13 || tag === 14) {
    skipSchemaValueType(reader);
    return;
  }
  if (tag === 15) {
    reader.readVec((field) => {
      field.option((name) => name.string());
      skipSchemaValueType(field);
    });
  }
}

export function skipGrooveValue(reader: PostcardReader): void {
  const tag = reader.u64();
  switch (tag) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 9:
      reader.u64();
      return;
    case 4:
      reader.f64Le();
      return;
    case 5:
      reader.bool();
      return;
    case 6:
      reader.string();
      return;
    case 7:
      reader.bytes();
      return;
    case 8:
      reader.bytes(false);
      return;
    case 10:
    case 11:
      reader.readVec(skipGrooveValue);
      return;
    case 12:
      reader.option(skipGrooveValue);
      return;
    case 13:
      reader.i64();
      return;
    default:
      throw new Error(`unsupported groove value tag ${tag}`);
  }
}

export type TestPolicyBranch = {
  filters: TestPolicyPredicate[];
  joins: TestPolicyJoin[];
};

export type TestPolicyInherits = {
  parentColumn: string;
  maxDepth?: number;
};

export type TestPolicyJoin = {
  table: string;
  onColumn: string;
  targetTag: number;
  sourceColumn: string | undefined;
  sourceLookup: { table: string; rowIdSourceColumn: string; valueColumn: string } | undefined;
  filters: TestPolicyPredicate[];
  nestedJoins: TestPolicyJoin[];
};

export type TestPolicyReachable = {
  accessTable: string;
  accessRowColumn: string;
  accessTeamColumn: string;
  accessTeamTargetTag: number;
  accessFilters: TestPolicyPredicate[];
  edgeTable: string;
  edgeMemberColumn: string;
  edgeParentColumn: string;
  edgeFilters: TestPolicyPredicate[];
  maxDepth: number;
  seed:
    | {
        table: string;
        userColumn: string | undefined;
        userClaim: string | undefined;
        teamColumn: string;
        filters: TestPolicyPredicate[];
      }
    | undefined;
};

export type TestPolicyPredicate =
  | { tag: number; children: TestPolicyPredicate[] }
  | { tag: number; child: TestPolicyPredicate }
  | { tag: number; column: string; operand: TestPolicyOperand }
  | { tag: number };

export type TestPolicyOperand =
  | { tag: number; column: string }
  | { tag: number; claim: string }
  | { tag: number; literalTag: number; value: unknown };
