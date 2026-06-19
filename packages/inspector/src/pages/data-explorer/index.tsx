import { useMemo } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { NavLink, Outlet, useOutletContext, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./index.module.css";

const TABLES_SIDEBAR_SIZE_STORAGE_KEY = "jazz.inspector.dataExplorer.tablesSidebarSize";
const TABLES_SIDEBAR_DEFAULT_SIZE = 10;
const TABLES_SIDEBAR_MIN_SIZE = 7;
const TABLES_SIDEBAR_MAX_SIZE = 30;

interface DataExplorerOutletContext {
  isTablesPanelOpen: boolean;
}

function getStoredTablesSidebarSize(): number {
  try {
    const rawSize = localStorage.getItem(TABLES_SIDEBAR_SIZE_STORAGE_KEY);
    if (rawSize === null) {
      return TABLES_SIDEBAR_DEFAULT_SIZE;
    }

    const storedSize = Number(rawSize);
    if (
      Number.isFinite(storedSize) &&
      storedSize >= TABLES_SIDEBAR_MIN_SIZE &&
      storedSize <= TABLES_SIDEBAR_MAX_SIZE
    ) {
      return storedSize;
    }
  } catch {
    // Ignore storage failures and keep the layout usable.
  }

  return TABLES_SIDEBAR_DEFAULT_SIZE;
}

function saveTablesSidebarSize(size: number): void {
  if (!Number.isFinite(size) || size < TABLES_SIDEBAR_MIN_SIZE || size > TABLES_SIDEBAR_MAX_SIZE) {
    return;
  }

  try {
    localStorage.setItem(TABLES_SIDEBAR_SIZE_STORAGE_KEY, String(size));
  } catch {
    // Ignore storage failures and keep resizing responsive.
  }
}

export function DataExplorer() {
  const {
    wasmSchema: schema,
    runtime,
    queryPropagation,
    setQueryPropagation,
  } = useDevtoolsContext();
  const isTablesPanelOpen =
    useOutletContext<DataExplorerOutletContext | null>()?.isTablesPanelOpen ?? true;
  const { table } = useParams();

  const tableNames = useMemo(() => Object.keys(schema ?? {}).sort(), [schema]);
  const defaultTablesSidebarSize = useMemo(getStoredTablesSidebarSize, []);

  return (
    <Group className={styles.layout} orientation="horizontal">
      {isTablesPanelOpen ? (
        <>
          <Panel
            id="tables-panel"
            className={styles.sidebarPanel}
            defaultSize={`${defaultTablesSidebarSize}%`}
            minSize={`${TABLES_SIDEBAR_MIN_SIZE}%`}
            maxSize={`${TABLES_SIDEBAR_MAX_SIZE}%`}
            onResize={(panelSize, _id, previousPanelSize) => {
              if (previousPanelSize === undefined) {
                return;
              }

              saveTablesSidebarSize(panelSize.asPercentage);
            }}
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
          <Outlet />
        </main>
      </Panel>
    </Group>
  );
}
