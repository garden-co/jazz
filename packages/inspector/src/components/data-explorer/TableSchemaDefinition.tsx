import { useMemo } from "react";
import { Link, Navigate, useParams } from "react-router";
import type { WasmSchema } from "jazz-tools";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./TableSchemaSql.module.css";

export function TableSchemaDefinition() {
  const { table } = useParams();

  if (!table) {
    return <Navigate to="/data-explorer" replace />;
  }

  const schema = useDevtoolsContext().wasmSchema;

  if (!schema) {
    return <p>No schema loaded for this connection.</p>;
  }

  const tableSchema = (schema as WasmSchema)[table];

  if (!tableSchema) {
    return <p>Unknown table: {table}</p>;
  }

  const formattedSchema = useMemo(() => {
    return JSON.stringify({ [table]: tableSchema }, null, 2);
  }, [table, tableSchema]);

  return (
    <section className={styles.container}>
      <header className={styles.header}>
        <Link
          to={`/data-explorer/${table}/data`}
          className={styles.backLink}
          aria-label="Back to data"
        >
          &larr;
        </Link>
        <h2 className={styles.title}>{table} schema</h2>
      </header>
      <pre className={styles.codeBlock}>
        <code>{formattedSchema}</code>
      </pre>
    </section>
  );
}
