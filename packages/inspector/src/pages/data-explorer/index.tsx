import { useMemo } from "react";
import { NavLink, Outlet, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./index.module.css";

export function DataExplorer() {
  const schema = useDevtoolsContext().wasmSchema;
  const { table } = useParams();

  const tableNames = useMemo(() => Object.keys(schema ?? {}).sort(), [schema]);

  return (
    <div className={styles.layout}>
      <aside className={styles.sidebar}>
        <h2 className={styles.sidebarTitle}>Tables</h2>
        <ul className={styles.tableList}>
          {tableNames.map((tableName) => (
            <li key={tableName}>
              <div className={styles.tableRow}>
                <NavLink
                  to={`/data-explorer/${tableName}/data`}
                  className={`${styles.tableLink} ${table === tableName ? styles.tableLinkActive : ""}`}
                  aria-label={`View ${tableName} data`}
                >
                  {tableName}
                </NavLink>
                <NavLink
                  to={`/data-explorer/${tableName}/schema`}
                  className={styles.schemaLink}
                  aria-label={`View ${tableName} schema`}
                >
                  <span className={styles.schemaIcon}>⌘</span>
                  <span className={styles.schemaLabel}>Schema</span>
                </NavLink>
              </div>
            </li>
          ))}
        </ul>
      </aside>
      <main className={styles.content}>
        {!table ? (
          <section className={styles.emptyState}>
            <h3 className={styles.emptyTitle}>Select a table</h3>
            <p className={styles.emptyText}>Choose a table from the left sidebar to view rows.</p>
          </section>
        ) : null}
        <Outlet />
      </main>
    </div>
  );
}
