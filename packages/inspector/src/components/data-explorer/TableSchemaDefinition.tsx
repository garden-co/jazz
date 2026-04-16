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

  const { runtime, storedPermissions, wasmSchema: schema } = useDevtoolsContext();

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

  const formattedPermissions = useMemo(() => {
    if (runtime === "extension") {
      return null;
    }
    if (!storedPermissions?.head) {
      return "No published sync-server permissions found for this app.";
    }
    const tablePermissions = storedPermissions.permissions?.[table];
    if (!tablePermissions) {
      return `No stored permissions for table "${table}".`;
    }
    return JSON.stringify({ [table]: tablePermissions }, null, 2);
  }, [runtime, storedPermissions, table]);

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
      <div className={styles.sections}>
        <section className={styles.panel}>
          <pre className={styles.codeBlock}>
            <code>{formattedSchema}</code>
          </pre>
        </section>
        {runtime === "standalone" ? (
          <section className={styles.panel}>
            <header className={styles.sectionHeader}>
              <h2 className={styles.sectionTitle}>{table} permissions</h2>
            </header>
            <pre className={styles.codeBlock}>
              <code>{formattedPermissions}</code>
            </pre>
          </section>
        ) : null}
      </div>
    </section>
  );
}
