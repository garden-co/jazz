// DSL for defining schemas and migrations
// ============================================================================
// Column Builder (for schema context)
// ============================================================================
class ColumnBuilder {
    _sqlType;
    _nullable = false;
    constructor(_sqlType) {
        this._sqlType = _sqlType;
    }
    optional() {
        this._nullable = true;
        return this;
    }
    _build(name) {
        return {
            name,
            sqlType: this._sqlType,
            nullable: this._nullable,
        };
    }
}
// ============================================================================
// Add Builder (for migration context)
// ============================================================================
class AddBuilder {
    string(opts) {
        return { _type: "add", sqlType: "TEXT", default: opts.default };
    }
    int(opts) {
        return { _type: "add", sqlType: "INTEGER", default: opts.default };
    }
    boolean(opts) {
        return { _type: "add", sqlType: "BOOLEAN", default: opts.default };
    }
    float(opts) {
        return { _type: "add", sqlType: "REAL", default: opts.default };
    }
}
// ============================================================================
// Drop Builder (for migration context)
// ============================================================================
class DropBuilder {
    string(opts) {
        return { _type: "drop", sqlType: "TEXT", backwardsDefault: opts.backwardsDefault };
    }
    int(opts) {
        return { _type: "drop", sqlType: "INTEGER", backwardsDefault: opts.backwardsDefault };
    }
    boolean(opts) {
        return { _type: "drop", sqlType: "BOOLEAN", backwardsDefault: opts.backwardsDefault };
    }
    float(opts) {
        return { _type: "drop", sqlType: "REAL", backwardsDefault: opts.backwardsDefault };
    }
}
// ============================================================================
// col namespace
// ============================================================================
export const col = {
    // Schema context
    string: () => new ColumnBuilder("TEXT"),
    boolean: () => new ColumnBuilder("BOOLEAN"),
    int: () => new ColumnBuilder("INTEGER"),
    float: () => new ColumnBuilder("REAL"),
    // Migration context
    add: () => new AddBuilder(),
    drop: () => new DropBuilder(),
    rename: (oldName) => ({ _type: "rename", oldName }),
};
// ============================================================================
// Side-effect collection
// ============================================================================
let collectedTables = [];
let collectedMigrations = [];
export function table(name, columns) {
    const cols = [];
    for (const [colName, builder] of Object.entries(columns)) {
        cols.push(builder._build(colName));
    }
    collectedTables.push({ name, columns: cols });
}
export function migrate(tableName, ops) {
    const operations = Object.entries(ops).map(([column, op]) => ({ column, op }));
    collectedMigrations.push({ table: tableName, operations });
}
export function getCollectedSchema() {
    const schema = { tables: [...collectedTables] };
    collectedTables = [];
    return schema;
}
export function getCollectedMigration() {
    if (collectedMigrations.length === 0) {
        return null;
    }
    const migration = collectedMigrations[0];
    collectedMigrations = [];
    const operations = migration.operations.map(({ column, op }) => {
        switch (op._type) {
            case "add":
                return { type: "introduce", column, value: op.default };
            case "drop":
                return { type: "drop", column, value: op.backwardsDefault };
            case "rename":
                return { type: "rename", column, value: op.oldName };
        }
    });
    return { table: migration.table, operations };
}
export function resetCollectedState() {
    collectedTables = [];
    collectedMigrations = [];
}
//# sourceMappingURL=dsl.js.map