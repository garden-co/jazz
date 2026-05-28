import { useMemo } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { Link, Outlet, useParams } from "@tanstack/react-router";
import { useOptionalInspectorLayoutContext } from "#inspector-layout/index";
import { appRoutes } from "#lib/navigation/appRoutes.ts";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./index.module.css";

interface DataExplorerProps {
  children?: React.ReactNode;
}

export function DataExplorer({ children }: DataExplorerProps = {}) {
  const {
    wasmSchema: schema,
    runtime,
    queryPropagation,
    setQueryPropagation,
  } = useDevtoolsContext();
  const isTablesPanelOpen = useOptionalInspectorLayoutContext()?.isTablesPanelOpen ?? true;
  const params = useParams({ strict: false });
  const table = params.tableName;

  const tableNames = useMemo(() => Object.keys(schema ?? {}).sort(), [schema]);

  return (
    <Group className={styles.layout} orientation="horizontal">
      {isTablesPanelOpen ? (
        <>
          <Panel
            id="tables-panel"
            className={styles.sidebarPanel}
            defaultSize="20%"
            minSize="14%"
            maxSize="30%"
          >
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
                    <Link
                      to={appRoutes.tableData}
                      params={{
                        connectionId: params.connectionId ?? "",
                        branch: params.branch ?? "",
                        schemaHash: params.schemaHash ?? "",
                        tableName,
                      }}
                      className={`${styles.tableLink} ${table === tableName ? styles.tableLinkActive : ""}`}
                      aria-label={`View ${tableName} data`}
                    >
                      {tableName}
                    </Link>
                  </li>
                ))}
              </ul>
            </aside>
          </Panel>
          <Separator className={styles.resizeHandle} />
        </>
      ) : null}
      <Panel id="data-explorer-content" className={styles.contentPanel} minSize="40%">
        <main className={styles.content}>
          {!table ? (
            <section className={styles.emptyState}>
              <h3 className={styles.emptyTitle}>Select a table</h3>
              <p className={styles.emptyText}>Choose a table from the left sidebar to view rows.</p>
            </section>
          ) : null}
          {children ?? <Outlet />}
        </main>
      </Panel>
    </Group>
  );
}
