// SQL generation from schema AST
function columnToSql(column) {
    const nullability = column.nullable ? "" : " NOT NULL";
    return `    ${column.name} ${column.sqlType}${nullability}`;
}
function tableToSql(table) {
    const columnDefs = table.columns.map(columnToSql);
    return `CREATE TABLE ${table.name} (\n${columnDefs.join(",\n")}\n);`;
}
export function schemaToSql(schema) {
    return schema.tables.map(tableToSql).join("\n\n") + "\n";
}
function formatDefaultValue(value) {
    if (typeof value === "string") {
        return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === "boolean") {
        return value ? "TRUE" : "FALSE";
    }
    if (typeof value === "number") {
        return String(value);
    }
    if (value === null) {
        return "NULL";
    }
    throw new Error(`Unsupported default value type: ${typeof value}`);
}
function lensOpToForwardSql(table, op) {
    switch (op.type) {
        case "introduce":
            return `ALTER TABLE ${table} ADD COLUMN ${op.column} TEXT DEFAULT ${formatDefaultValue(op.value)};`;
        case "drop":
            return `ALTER TABLE ${table} DROP COLUMN ${op.column};`;
        case "rename":
            return `ALTER TABLE ${table} RENAME COLUMN ${op.column} TO ${op.value};`;
    }
}
function lensOpToBackwardSql(table, op) {
    switch (op.type) {
        case "introduce":
            return `ALTER TABLE ${table} DROP COLUMN ${op.column};`;
        case "drop":
            return `ALTER TABLE ${table} ADD COLUMN ${op.column} TEXT DEFAULT ${formatDefaultValue(op.value)};`;
        case "rename":
            return `ALTER TABLE ${table} RENAME COLUMN ${op.value} TO ${op.column};`;
    }
}
export function lensToSql(lens, direction) {
    const converter = direction === "fwd" ? lensOpToForwardSql : lensOpToBackwardSql;
    return lens.operations.map((op) => converter(lens.table, op)).join("\n") + "\n";
}
//# sourceMappingURL=sql-gen.js.map