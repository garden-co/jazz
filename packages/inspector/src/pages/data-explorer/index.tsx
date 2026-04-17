import { useMemo } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { NavLink, Outlet, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./index.module.css";

export function DataExplorer() {
  const {
    wasmSchema: schema,
    runtime,
    queryPropagation,
    setQueryPropagation,
  } = useDevtoolsContext();
  const { table } = useParams();

  const tableNames = useMemo(() => Object.keys(schema ?? {}).sort(), [schema]);

  return (
    <Group className={styles.layout} orientation="horizontal">
      <Panel className={styles.sidebarPanel} defaultSize="20%" minSize="14%" maxSize="30%">
        <aside className={styles.sidebar}>
          <div className={styles.sidebarHeader}>
            <h2 className={styles.sidebarTitle}>Tables</h2>
            {runtime === "extension" ? (
              <label className={styles.propagationSwitch}>
                <span className={styles.propagationLabel}>Local-only</span>
                <input
                  type="checkbox"
                  checked={queryPropagation === "local-only"}
                  onChange={(event) => {
                    setQueryPropagation(event.target.checked ? "local-only" : "full");
                  }}
                  aria-label="Toggle query propagation between local-only and full"
                />
              </label>
            ) : null}
          </div>
          <ul className={styles.tableList}>
            {tableNames.map((tableName) => (
              <li key={tableName}>
                <NavLink
                  to={`/data-explorer/${tableName}/data`}
                  className={`${styles.tableLink} ${table === tableName ? styles.tableLinkActive : ""}`}
                  aria-label={`View ${tableName} data`}
                >
                  {tableName}
                </NavLink>
              </li>
            ))}
          </ul>
        </aside>
      </Panel>
      <Separator className={styles.resizeHandle} />
      <Panel className={styles.contentPanel} minSize="40%">
        <main className={styles.content}>
          {!table ? (
            <section className={styles.emptyState}>
              <h3 className={styles.emptyTitle}>Select a table</h3>
              <p className={styles.emptyText}>Choose a table from the left sidebar to view rows.</p>
            </section>
          ) : null}
          <Outlet />
        </main>
      </Panel>
    </Group>
  );
}
