/** Explicit, author-owned relationship declarations. Names are never inferred. */
export interface ForwardRelationship<
  TTable extends string = string,
  TColumn extends string = string,
> {
  readonly kind: "forward";
  readonly table: TTable;
  readonly column: TColumn;
}
export interface ReverseRelationship<
  TTable extends string = string,
  TRelation extends string = string,
> {
  readonly kind: "reverse";
  readonly table: TTable;
  readonly relation: TRelation;
}
export type Relationship = ForwardRelationship | ReverseRelationship;
export type Relationships = Record<string, Relationship>;

export function rel<const TTable extends string, const TColumn extends string>(
  table: TTable,
  column: TColumn,
): ForwardRelationship<TTable, TColumn> {
  if (!table || !column)
    throw new Error("s.rel(table, column) requires nonempty table and column names.");
  return { kind: "forward", table, column };
}
export function reverse<const TTable extends string, const TRelation extends string>(
  table: TTable,
  relation: TRelation,
): ReverseRelationship<TTable, TRelation> {
  if (!table || !relation)
    throw new Error("s.reverse(table, relation) requires a table and named forward relation.");
  return { kind: "reverse", table, relation };
}
