/**
 * `s.table(columns, relations)`: a table of the app's schema, as .64 wrote
 * it. A table is only a description: `defineSchema` turns the tables into
 * `jazz.app.json`'s readable form (`readable-app.ts`).
 */
import type { AnyColumnDefinition, ColumnBuilderSqlType, ComputedColumn } from "../dsl.js";
import { isComputedColumn } from "../dsl.js";
import {
  assertRelationshipDeclaration,
  type ForwardRelationship,
  type Relationships,
} from "../relationships.js";
import { assertUserTableColumnNameAllowed, type NoExplicitIdColumn } from "../magic-columns.js";
import type { VectorSqlType } from "../schema.js";

export type TableDefinition = Record<string, AnyColumnDefinition> & NoExplicitIdColumn;

/** The stored columns' names: every column but the computed fields. */
export type StoredColumnKeys<C> = {
  [K in keyof C & string]: C[K] extends ComputedColumn<any, any> ? never : K;
}[keyof C & string];

/**
 * Provenance timestamps every row carries. A composite index may use them,
 * so a list's newest page (`roomId = ? ORDER BY $createdAt DESC`) is served
 * from the index instead of sorting the whole list.
 */
const PROVENANCE_INDEX_COLUMNS = ["$createdAt", "$updatedAt"] as const;
type ProvenanceIndexColumn = (typeof PROVENANCE_INDEX_COLUMNS)[number];
type CompositeIndexColumn<C> = StoredColumnKeys<C> | ProvenanceIndexColumn;

/** The stored columns whose type is `Sql`. */
type ColumnsTyped<C, Sql> = {
  [K in StoredColumnKeys<C>]: C[K] extends AnyColumnDefinition
    ? ColumnBuilderSqlType<C[K]> extends Sql
      ? K
      : never
    : never;
}[StoredColumnKeys<C>];

/**
 * A text index field: its weight (1 to 15), or that and its reference
 * length in words (32 unless given, about 256 suits long text).
 */
export type TextIndexField = number | { readonly weight: number; readonly length?: number };

/** How a text index reads text. */
export interface TextIndexOptions {
  /** How text becomes words: English with stemming, or words as written. */
  readonly analyzer: "jazz-text/1:en" | "jazz-text/1:simple";
  /** Whether a query's last word also matches as a prefix. */
  readonly prefix?: boolean;
}

/** How a vector index compares. */
export interface VectorIndexOptions {
  readonly metric: "cosine" | "l2" | "dot";
}

/** A specialised index as `jazz.app.json`'s `indexes` lists it (SPEC 20.1). */
export type SpecialisedIndex =
  | ({
      readonly name: string;
      readonly text: Readonly<Record<string, TextIndexField>>;
    } & TextIndexOptions)
  | ({ readonly name: string; readonly vector: string } & VectorIndexOptions)
  | { readonly name: string; readonly geo: string };

interface TableOptions<TColumns> {
  readonly indexedColumns?: readonly (keyof TColumns & string)[];
  readonly branchColumns?: readonly (keyof TColumns & string)[];
  readonly compositeIndexes?: readonly (readonly string[])[];
  readonly specialisedIndexes?: readonly SpecialisedIndex[];
  readonly historyRetention?: "latest";
}

/** A table: its columns (computed fields included), its relations and its options. */
export class DefinedTable<
  TColumns extends TableDefinition = TableDefinition,
  TRelations extends Relationships = Relationships,
> {
  public readonly __jazzTableDefinition = true as const;
  public readonly indexedColumns?: readonly (keyof TColumns & string)[];
  public readonly branchColumns?: readonly (keyof TColumns & string)[];
  public readonly compositeIndexes?: readonly (readonly string[])[];
  public readonly specialisedIndexes?: readonly SpecialisedIndex[];
  public readonly historyRetention?: "latest";

  constructor(
    public readonly columns: TColumns,
    public readonly relations: TRelations,
    options: TableOptions<TColumns> = {},
  ) {
    Object.assign(this, options);
  }

  private options(): TableOptions<TColumns> {
    return {
      indexedColumns: this.indexedColumns,
      branchColumns: this.branchColumns,
      compositeIndexes: this.compositeIndexes,
      specialisedIndexes: this.specialisedIndexes,
      historyRetention: this.historyRetention,
    };
  }

  private storedColumn(method: string, column: string): void {
    if (!Object.hasOwn(this.columns, column) || isComputedColumn(this.columns[column]!))
      throw new Error(`table.${method}(...) references unknown column "${column}".`);
  }

  /** Index only these columns (by default every column has a single-column index). */
  indexOnly<
    const TColumnsForIndex extends readonly [
      StoredColumnKeys<TColumns>,
      ...StoredColumnKeys<TColumns>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations> {
    for (const column of columns) this.storedColumn("indexOnly", column);
    return new DefinedTable(this.columns, this.relations, {
      ...this.options(),
      indexedColumns: [...columns],
    });
  }

  /**
   * An ordered index over two or more columns, e.g. `["roomId", "$createdAt"]`
   * for a room's messages newest first. Columns are declared, stored columns
   * or the provenance timestamps `$createdAt` and `$updatedAt`.
   */
  compositeIndex<
    const TColumnsForIndex extends readonly [
      CompositeIndexColumn<TColumns>,
      CompositeIndexColumn<TColumns>,
      ...CompositeIndexColumn<TColumns>[],
    ],
  >(columns: TColumnsForIndex): DefinedTable<TColumns, TRelations> {
    const indexable = (column: string) =>
      (PROVENANCE_INDEX_COLUMNS as readonly string[]).includes(column) ||
      (Object.hasOwn(this.columns, column) && !isComputedColumn(this.columns[column]!));
    if (new Set(columns).size !== columns.length || !columns.every(indexable)) {
      throw new Error(
        "table.compositeIndex(...) requires distinct declared columns or provenance timestamps ($createdAt, $updatedAt).",
      );
    }
    const key = JSON.stringify(columns);
    if ((this.compositeIndexes ?? []).some((existing) => JSON.stringify(existing) === key)) {
      throw new Error("table.compositeIndex(...) declares the same index twice.");
    }
    return new DefinedTable(this.columns, this.relations, {
      ...this.options(),
      compositeIndexes: [...(this.compositeIndexes ?? []), [...columns]],
    });
  }

  private withIndex(
    method: string,
    index: SpecialisedIndex,
    columns: readonly string[],
  ): DefinedTable<TColumns, TRelations> {
    for (const column of columns) this.storedColumn(method, column);
    if ((this.specialisedIndexes ?? []).some((existing) => existing.name === index.name))
      throw new Error(`table.${method}(...) declares a second index named "${index.name}".`);
    return new DefinedTable(this.columns, this.relations, {
      ...this.options(),
      specialisedIndexes: [...(this.specialisedIndexes ?? []), index],
    });
  }

  /**
   * Word search over string columns, each with a weight, searched by `name`.
   *
   * @example
   * ```ts
   * .textIndex("search", { title: 4, body: { weight: 1, length: 256 } }, { analyzer: "jazz-text/1:en" })
   * ```
   */
  textIndex(
    name: string,
    fields: { readonly [K in ColumnsTyped<TColumns, "TEXT">]?: TextIndexField },
    options: TextIndexOptions,
  ): DefinedTable<TColumns, TRelations> {
    const text = { ...(fields as Record<string, TextIndexField>) };
    return this.withIndex("textIndex", { name, text, ...options }, Object.keys(text));
  }

  /**
   * Nearest-neighbour search over a vector column, searched by `name`.
   *
   * @example
   * ```ts
   * .vectorIndex("similar", "embedding", { metric: "cosine" })
   * ```
   */
  vectorIndex(
    name: string,
    column: ColumnsTyped<TColumns, VectorSqlType>,
    options: VectorIndexOptions,
  ): DefinedTable<TColumns, TRelations> {
    return this.withIndex("vectorIndex", { name, vector: column, ...options }, [column]);
  }

  /**
   * Radius and box search over a point column, searched by `name`.
   *
   * @example
   * ```ts
   * .geoIndex("near", "location")
   * ```
   */
  geoIndex(
    name: string,
    column: ColumnsTyped<TColumns, "POINT">,
  ): DefinedTable<TColumns, TRelations> {
    return this.withIndex("geoIndex", { name, geo: column }, [column]);
  }

  /** Branch rows by these columns' values. */
  branchBy<const TBranchColumn extends StoredColumnKeys<TColumns>>(
    column: TBranchColumn,
  ): DefinedTable<TColumns, TRelations>;
  branchBy<
    const TBranchColumns extends readonly [
      StoredColumnKeys<TColumns>,
      ...StoredColumnKeys<TColumns>[],
    ],
  >(columns: TBranchColumns): DefinedTable<TColumns, TRelations>;
  branchBy(columns: string | readonly string[]): DefinedTable<TColumns, TRelations> {
    const normalized = typeof columns === "string" ? [columns] : [...columns];
    for (const column of normalized) this.storedColumn("branchBy", column);
    return new DefinedTable(this.columns, this.relations, {
      ...this.options(),
      branchColumns: normalized as (keyof TColumns & string)[],
    });
  }

  /**
   * Keep only each row's latest version, for state that is overwritten
   * often, like cursors and presence. Older versions are dropped, so the
   * table cannot be read through history, snapshots or branches, and it
   * cannot have text-merging columns.
   */
  history(retention: "latest"): DefinedTable<TColumns, TRelations> {
    if (retention !== "latest") {
      throw new Error(
        `table.history(...) supports only "latest", not ${JSON.stringify(retention)}.`,
      );
    }
    return new DefinedTable(this.columns, this.relations, {
      ...this.options(),
      historyRetention: retention,
    });
  }
}

type RelationColumns<T> = {
  [K in StoredColumnKeys<T>]: T[K] extends AnyColumnDefinition
    ? ColumnBuilderSqlType<T[K]> extends "UUID" | { kind: "ARRAY"; element: "UUID" }
      ? K
      : never
    : never;
}[StoredColumnKeys<T>];
type ConflictingTargets<R extends Relationships, C extends string, T extends string> = {
  [N in keyof R]: R[N] extends ForwardRelationship<infer OtherTarget, C>
    ? OtherTarget extends T
      ? never
      : N
    : never;
}[keyof R];
/** A relation may not take a column's name, and a forward one needs a uuid column. */
type ValidateRelations<C, R extends Relationships> = {
  [K in keyof R]: K extends
    | keyof C
    | "id"
    | "__proto__"
    | "constructor"
    | "prototype"
    | ""
    | `$${string}`
    ? never
    : R[K] extends ForwardRelationship<infer Target, infer Col>
      ? Col extends RelationColumns<C>
        ? [ConflictingTargets<R, Col, Target>] extends [never]
          ? R[K]
          : never
        : never
      : R[K];
};
/** A computed field may read only stored columns (and `id`). */
type ValidateComputed<C> = {
  [K in keyof C]: C[K] extends ComputedColumn<any, infer From>
    ? [Exclude<From, StoredColumnKeys<C> | "id">] extends [never]
      ? C[K]
      : ComputedColumn<any, StoredColumnKeys<C> | "id">
    : C[K];
};

/**
 * Define a table: its columns (`s.string()`, …, and `s.computed(...)`
 * fields) and its relations (`s.rel`, `s.reverse`).
 *
 * @example
 * ```ts
 * export const schema = s.defineSchema({
 *   todos: s.table({ title: s.string(), projectId: s.uuid() }, { project: s.rel("projects", "projectId") }),
 *   projects: s.table({ name: s.string() }, { todos: s.reverse("todos", "project") }),
 * });
 * ```
 */
export function defineTable<
  const TColumns extends TableDefinition,
  const TRelations extends Relationships = {},
>(
  columns: TColumns & ValidateComputed<TColumns>,
  relations?: TRelations & ValidateRelations<TColumns, TRelations>,
): DefinedTable<TColumns, TRelations> {
  if (!columns || typeof columns !== "object" || Array.isArray(columns))
    throw new Error("s.table(columns, relations) requires an object of columns.");
  const rels = (relations ?? {}) as Relationships;
  if (typeof rels !== "object" || Array.isArray(rels))
    throw new Error("s.table(columns, relations): relations must be an object; use {} for none.");
  for (const [name, column] of Object.entries(columns as Record<string, AnyColumnDefinition>)) {
    assertUserTableColumnNameAllowed(name);
    if (isComputedColumn(column)) {
      for (const from of column._computed.from) {
        if (from === "id") continue;
        const source = (columns as Record<string, AnyColumnDefinition>)[from];
        if (!Object.hasOwn(columns, from) || !source || isComputedColumn(source))
          throw new Error(
            `Computed field "${name}" reads "${from}", which is not a stored column of its table.`,
          );
      }
    }
  }
  const targets = new Map<string, string>();
  for (const [name, relation] of Object.entries(rels)) {
    if (
      !name ||
      ["__proto__", "constructor", "prototype"].includes(name) ||
      name.startsWith("$") ||
      name === "id" ||
      Object.hasOwn(columns, name)
    )
      throw new Error(`Relation "${name}" collides with a column.`);
    assertRelationshipDeclaration(relation, name);
    if (relation.kind !== "forward") continue;
    const previous = targets.get(relation.column);
    if (previous && previous !== relation.table)
      throw new Error(`Conflicting relation targets for column "${relation.column}".`);
    targets.set(relation.column, relation.table);
    const builder = (columns as Record<string, AnyColumnDefinition>)[relation.column];
    if (!Object.hasOwn(columns, relation.column) || !builder || isComputedColumn(builder))
      throw new Error(`Relation "${name}" references unknown column "${relation.column}".`);
    const sqlType = (builder as unknown as { _sqlType: unknown })._sqlType;
    const uuidArray =
      typeof sqlType === "object" &&
      sqlType !== null &&
      (sqlType as { kind?: string }).kind === "ARRAY" &&
      (sqlType as { element?: unknown }).element === "UUID";
    if (sqlType !== "UUID" && !uuidArray)
      throw new Error(
        `Relation "${name}" requires a uuid or uuid-array column; "${relation.column}" is not one.`,
      );
  }
  return new DefinedTable(columns as TColumns, rels as TRelations);
}

export function isDefinedTable(value: unknown): value is DefinedTable {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { __jazzTableDefinition?: unknown }).__jazzTableDefinition === true
  );
}
