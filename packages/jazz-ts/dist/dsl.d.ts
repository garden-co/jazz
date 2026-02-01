import type { Column, Schema, SqlType, Lens, AddOp, DropOp, RenameOp, MigrationOp } from "./schema.js";
declare class ColumnBuilder {
    private _sqlType;
    private _nullable;
    constructor(_sqlType: SqlType);
    optional(): this;
    _build(name: string): Column;
}
declare class AddBuilder {
    string(opts: {
        default: string;
    }): AddOp;
    int(opts: {
        default: number;
    }): AddOp;
    boolean(opts: {
        default: boolean;
    }): AddOp;
    float(opts: {
        default: number;
    }): AddOp;
}
declare class DropBuilder {
    string(opts: {
        backwardsDefault: string;
    }): DropOp;
    int(opts: {
        backwardsDefault: number;
    }): DropOp;
    boolean(opts: {
        backwardsDefault: boolean;
    }): DropOp;
    float(opts: {
        backwardsDefault: number;
    }): DropOp;
}
export declare const col: {
    string: () => ColumnBuilder;
    boolean: () => ColumnBuilder;
    int: () => ColumnBuilder;
    float: () => ColumnBuilder;
    add: () => AddBuilder;
    drop: () => DropBuilder;
    rename: (oldName: string) => RenameOp;
};
export declare function table(name: string, columns: Record<string, ColumnBuilder>): void;
export declare function migrate(tableName: string, ops: Record<string, MigrationOp>): void;
export declare function getCollectedSchema(): Schema;
export declare function getCollectedMigration(): Lens | null;
export declare function resetCollectedState(): void;
export {};
//# sourceMappingURL=dsl.d.ts.map