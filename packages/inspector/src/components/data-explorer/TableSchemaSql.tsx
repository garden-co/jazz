import { useMemo } from "react";
import { Navigate, useParams } from "react-router";
import { schemaToSql, type Schema } from "jazz-tools";
import { wasmTableToJazzSchema, type WasmTableSchemaLike } from "../../utility/wasm-schema-sql.js";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./TableSchemaSql.module.css";

export function TableSchemaSql() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const schema = useDevtoolsContext().wasmSchema;

  if (!schema) {
    return <p>No schema loaded for this connection.</p>;
  }

  const tableSchema = (schema.tables as Record<string, WasmTableSchemaLike | undefined>)[table];

  if (!tableSchema) {
    return <p>Unknown table: {table}</p>;
  }

  const jazzSchema = useMemo<Schema>(() => {
    return wasmTableToJazzSchema(table, tableSchema);
  }, [table, tableSchema]);

  const sql = useMemo(() => {
    try {
      return schemaToSql(jazzSchema);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return `-- Failed to render SQL for table "${table}".\n-- ${message}`;
    }
  }, [jazzSchema, table]);

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <h2 className={styles.title}>{table} schema</h2>
      </header>
      <pre className={styles.codeBlock}>
        <code>{sql}</code>
      </pre>
    </section>
  );
}
