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
  if (typeof table !== "string" || !table || typeof column !== "string" || !column)
    throw new Error("s.rel(table, column) requires nonempty table and column names.");
  return { kind: "forward", table, column };
}
export function reverse<const TTable extends string, const TRelation extends string>(
  table: TTable,
  relation: TRelation,
): ReverseRelationship<TTable, TRelation> {
  if (typeof table !== "string" || !table || typeof relation !== "string" || !relation)
    throw new Error("s.reverse(table, relation) requires a table and named forward relation.");
  return { kind: "reverse", table, relation };
}

/** Validate untyped/serialized authoring metadata without coercing names. */
export function assertRelationshipDeclaration(
  value: unknown,
  context: string,
): asserts value is Relationship {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error(`Invalid relationship "${context}"; use s.rel(...) or s.reverse(...).`);
  const declaration = value as Record<string, unknown>;
  const key =
    declaration.kind === "forward"
      ? "column"
      : declaration.kind === "reverse"
        ? "relation"
        : undefined;
  if (
    !key ||
    typeof declaration.table !== "string" ||
    !declaration.table ||
    typeof declaration[key] !== "string" ||
    !declaration[key]
  )
    throw new Error(
      `Invalid relationship "${context}": table and ${key ?? "column/relation"} must be nonempty strings.`,
    );
}
