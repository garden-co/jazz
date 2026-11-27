// The client's query builder: chains a read (filter, sort, page, include a
// related row) into a QuerySpec for POST /api/query. It keeps the call
// shapes the copied screens use, so they swap only their imports.
import { tables, type Condition, type Include, type QuerySpec, type Rows, type TableName } from "../../schema";

// Included rows are typed loosely: the screens read the fields they asked for.
type Related = any;

export class Query<T extends TableName, R = Rows[T]> {
  constructor(readonly spec: QuerySpec & { table: T }) {}

  private with(change: Partial<QuerySpec>) {
    return new Query<T, R>({ ...this.spec, ...change } as QuerySpec & { table: T });
  }
  where(where: Partial<Record<keyof Rows[T] & string, Condition>>) {
    return this.with({ where: { ...this.spec.where, ...(where as Record<string, Condition>) } });
  }
  orderBy(column: keyof Rows[T] & string, direction: "asc" | "desc" = "asc") {
    return this.with({ orderBy: [...this.spec.orderBy, [column, direction]] });
  }
  include<I extends Include>(include: I) {
    return new Query<T, R & { [K in keyof I]?: Related | null }>({ ...this.spec, include: { ...this.spec.include, ...include } });
  }
  select(...columns: (keyof Rows[T] & string)[]) {
    return this.with({ select: columns });
  }
  limit(limit: number) {
    return this.with({ limit });
  }
  offset(offset: number) {
    return this.with({ offset });
  }

  /** The live topic: the label the query is scoped to, or your own (`me`). */
  get topic() {
    const organizationId = this.spec.table === "organizations" ? this.spec.where.id : this.spec.where.organizationId;
    return typeof organizationId === "string" ? `org:${organizationId}` : "me";
  }

  /** The tables the result depends on: its own and every included one. */
  get tables() {
    const found = new Set<string>([this.spec.table]);
    const walk = (table: TableName, include: Include) => {
      for (const [name, nested] of Object.entries(include)) {
        const target = (tables[table].relations as Record<string, readonly [TableName, string]>)[name]![0];
        found.add(target);
        if (nested !== true) walk(target, nested);
      }
    };
    walk(this.spec.table, this.spec.include);
    return [...found];
  }
}

const query = <T extends TableName>(table: T) => new Query<T>({ table, where: {}, orderBy: [], include: {} });

export const app = {
  organizations: query("organizations"),
  people: query("people"),
  memberships: query("memberships"),
  teams: query("teams"),
  teamAssignments: query("teamAssignments"),
  artists: query("artists"),
  catalogues: query("catalogues"),
  releases: query("releases"),
  releaseTeams: query("releaseTeams"),
  releaseAssignments: query("releaseAssignments"),
};

/** JSON has no dates: turn the release dates back into Dates, also in included rows. */
export function revive(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(revive);
  if (value && typeof value === "object") {
    const row = value as Record<string, unknown>;
    for (const [key, field] of Object.entries(row)) {
      if (key === "releaseDate" && typeof field === "string") row[key] = new Date(field);
      else if (field && typeof field === "object") revive(field);
    }
  }
  return value;
}
