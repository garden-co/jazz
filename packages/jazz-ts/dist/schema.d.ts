export type SqlType = "TEXT" | "BOOLEAN" | "INTEGER" | "REAL";
export interface Column {
    name: string;
    sqlType: SqlType;
    nullable: boolean;
}
export interface Table {
    name: string;
    columns: Column[];
}
export interface Schema {
    tables: Table[];
}
export interface AddOp {
    _type: "add";
    sqlType: SqlType;
    default: unknown;
}
export interface DropOp {
    _type: "drop";
    sqlType: SqlType;
    backwardsDefault: unknown;
}
export interface RenameOp {
    _type: "rename";
    oldName: string;
}
export type MigrationOp = AddOp | DropOp | RenameOp;
export interface TableMigration {
    table: string;
    operations: MigrationOpEntry[];
}
export interface MigrationOpEntry {
    column: string;
    op: MigrationOp;
}
export type LensOpType = "introduce" | "drop" | "rename";
export interface LensOp {
    type: LensOpType;
    column: string;
    value: unknown;
}
export interface Lens {
    table: string;
    operations: LensOp[];
}
//# sourceMappingURL=schema.d.ts.map