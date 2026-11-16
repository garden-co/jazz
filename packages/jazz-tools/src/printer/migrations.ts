/**
 * `jazz.app.json`'s `history` and `migrations` → `migrations/*.ts`
 * (`m.defineMigration`, the .64 stub shape) and `migrations/snapshots/*.json`
 * (the engine schema of each version, as `migrations create` writes them).
 */
import type { AppJson, ColumnJson, MigrateOpJson, MigrationJson, TableJson } from "./app-json.js";
import { member, propKey } from "./names.js";
import type { Unsupported } from "./permissions.js";
import { printDefault, printTable } from "./schema.js";

/** A version's engine snapshot: its full schema hash and the JSON `migrations create` writes. */
export interface Snapshot {
  hash: string;
  schema: unknown;
}

export interface MigrationsPrintOptions {
  /** The engine snapshot of a version's tables (`snapshotFromVersion`). */
  snapshot: (tables: Record<string, TableJson>) => Snapshot | Promise<Snapshot>;
  /** The time stamp of the first file; later files follow a second apart. Default: now. */
  start?: Date;
}

export interface PrintedMigrations {
  /** Relative path → contents. */
  files: Record<string, string>;
  unsupported: Unsupported[];
}

export function shortHash(hash: string): string {
  return hash.slice(0, 12);
}

/** `20261104T192647`, as `migrations create` names files. */
export function stamp(date: Date): string {
  return date
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d+Z$/, "");
}

export async function printMigrations(
  app: AppJson,
  options: MigrationsPrintOptions,
): Promise<PrintedMigrations> {
  const files: Record<string, string> = {};
  const unsupported: Unsupported[] = [];
  const versions = app.history ?? {};
  const migrations = app.migrations ?? [];
  if (Object.keys(versions).length === 0 && migrations.length === 0) return { files, unsupported };

  let time = (options.start ?? new Date()).getTime();
  const nextStamp = () => {
    const s = stamp(new Date(time));
    time += 1000;
    return s;
  };

  const snapshots = new Map<string, Snapshot>();
  for (const [name, version] of Object.entries(versions)) {
    snapshots.set(name, await options.snapshot(version.tables));
  }
  const current = await options.snapshot(app.tables);
  const tablesOf = (version: string | undefined) =>
    version === undefined ? app.tables : versions[version]?.tables;

  for (const [name, snapshot] of [...snapshots, ["", current] as const]) {
    void name;
    files[`migrations/snapshots/${nextStamp()}-${shortHash(snapshot.hash)}.json`] =
      JSON.stringify(snapshot.schema, null, 2) + "\n";
  }

  migrations.forEach((migration, index) => {
    const path = `migrations[${index}]`;
    const from = tablesOf(migration.from);
    const to = tablesOf(migration.to);
    const fromSnap = snapshots.get(migration.from);
    const toSnap = migration.to === undefined ? current : snapshots.get(migration.to);
    if (!from || !to || !fromSnap || !toSnap) {
      unsupported.push({
        path,
        text: JSON.stringify(migration),
        reason: "names a version `history` doesn't have",
      });
      return;
    }
    const fromHash = shortHash(fromSnap.hash);
    const toHash = shortHash(toSnap.hash);
    const slug = migration.name ? `${migration.name}-` : "";
    files[`migrations/${nextStamp()}-${slug}${fromHash}-${toHash}.ts`] = printStub(
      migration,
      from,
      to,
      fromHash,
      toHash,
      path,
      unsupported,
    );
  });
  return { files, unsupported };
}

function printStub(
  migration: MigrationJson,
  from: Record<string, TableJson>,
  to: Record<string, TableJson>,
  fromHash: string,
  toHash: string,
  path: string,
  unsupported: Unsupported[],
): string {
  const sections: string[] = [];
  if (migration.renameTables && Object.keys(migration.renameTables).length) {
    const body = Object.entries(migration.renameTables)
      .map(([to, from]) => `${propKey(to)}: m.renameTableFrom(${JSON.stringify(from)}),`)
      .join("\n");
    sections.push(`renameTables: {\n${body}\n},`);
  }
  if (migration.createTables?.length) {
    sections.push(
      `createTables: { ${migration.createTables.map((t) => `${propKey(t)}: true`).join(", ")} },`,
    );
  }
  if (migration.dropTables?.length) {
    sections.push(
      `dropTables: { ${migration.dropTables.map((t) => `${propKey(t)}: true`).join(", ")} },`,
    );
  }
  if (migration.migrate && Object.keys(migration.migrate).length) {
    const tables = Object.entries(migration.migrate).map(([table, columns]) => {
      const ops = Object.entries(columns).map(
        ([column, op]) =>
          `${propKey(column)}: ${printOp(op, `${path}.migrate.${table}.${column}`, unsupported)},`,
      );
      return `${propKey(table)}: {\n${ops.join("\n")}\n},`;
    });
    sections.push(`migrate: {\n${tables.join("\n")}\n},`);
  }
  sections.push(`fromHash: ${JSON.stringify(fromHash)},`);
  sections.push(`toHash: ${JSON.stringify(toHash)},`);
  const [witnessFrom, witnessTo] = witnesses(migration, from, to);
  const witness = (tables: Record<string, TableJson>, at: string) =>
    `{\n${Object.entries(tables)
      .map(
        ([name, table]) =>
          `${propKey(name)}: ${printTable(table, { relations: false, path: `${at}.${name}`, unsupported })},`,
      )
      .join("\n")}\n}`;
  sections.push(`from: ${witness(witnessFrom, `${path}.from`)},`);
  sections.push(`to: ${witness(witnessTo, `${path}.to`)},`);
  const comment = migration.$comment
    ? migration.$comment
        .split("\n")
        .map((l) => `// ${l}`.trimEnd())
        .join("\n") + "\n"
    : "";
  return `${comment}import { schema as s, migration as m } from "jazz-tools";\n\nexport default m.defineMigration({\n${sections.join("\n")}\n});\n`;
}

/** The tables a migration touches, on each side: what `migrations create` writes as witnesses. */
function witnesses(
  migration: MigrationJson,
  from: Record<string, TableJson>,
  to: Record<string, TableJson>,
): [Record<string, TableJson>, Record<string, TableJson>] {
  const names = new Set<string>();
  const renamedFrom = new Map(Object.entries(migration.renameTables ?? {}));
  for (const name of new Set([...Object.keys(from), ...Object.keys(to)])) {
    const a = from[renamedFrom.get(name) ?? name];
    const b = to[name];
    if (!a || !b || JSON.stringify(a.columns) !== JSON.stringify(b.columns)) names.add(name);
  }
  for (const table of Object.keys(migration.migrate ?? {})) names.add(table);
  const pick = (tables: Record<string, TableJson>, rename: (n: string) => string) =>
    Object.fromEntries(
      [...names]
        .map(rename)
        .filter((n) => tables[n])
        .map((n) => [n, stripRelations(tables[n]!)]),
    );
  return [pick(from, (n) => renamedFrom.get(n) ?? n), pick(to, (n) => n)];
}

function stripRelations(table: TableJson): TableJson {
  const { relations: _relations, computed: _computed, ...rest } = table;
  return rest;
}

function printOp(op: MigrateOpJson, path: string, unsupported: Unsupported[]): string {
  if ("renameFrom" in op) return `m.renameFrom(${JSON.stringify(op.renameFrom)})`;
  if ("add" in op)
    return printTypedOp("add", op.add, "default", op.default ?? null, path, unsupported);
  return printTypedOp(
    "drop",
    op.drop,
    "backwardsDefault",
    op.backwardsDefault ?? null,
    path,
    unsupported,
  );
}

function printTypedOp(
  kind: "add" | "drop",
  type: ColumnJson,
  key: string,
  value: unknown,
  path: string,
  unsupported: Unsupported[],
): string {
  const fn = member("m", kind);
  const def = `${key}: ${printDefault(type, value)}`;
  if (typeof value === "string" && value.startsWith("TODO:")) {
    unsupported.push({ path, text: value, reason: "a migration with a TODO left to the author" });
  }
  if (typeof type === "string") return `${fn}.${type.replace(/\?$/, "")}({ ${def} })`;
  if (type.type) {
    if (type.references) return `${fn}.ref(${JSON.stringify(type.references)}, { ${def} })`;
    return `${fn}.${type.type}({ ${def} })`;
  }
  if (type.enum)
    return `${fn}.enum(${type.enum.map((v) => JSON.stringify(v)).join(", ")}, { ${def} })`;
  if (type.json !== undefined) {
    return type.json === true
      ? `${fn}.json({ ${def} })`
      : `${fn}.json({ ${def}, schema: ${JSON.stringify(type.json)} })`;
  }
  if (type.array !== undefined) {
    const of = printTable(
      { columns: { x: type.array } },
      { relations: false, path, unsupported },
    ).match(/x: (.*),\n/)?.[1];
    return `${fn}.array({ of: ${of}, ${def} })`;
  }
  unsupported.push({
    path,
    text: JSON.stringify(type),
    reason: "unknown column type in a migration",
  });
  return `${fn}.string({ ${def} })`;
}
