import { useMemo } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { NavLink, Outlet, useOutletContext, useParams } from "react-router";
import type { QueryPropagation } from "jazz-tools";
import { useDevtoolsContext, type InspectorRuntime } from "../../contexts/devtools-context.js";
import { useLocalStorageState } from "../../utility/use-local-storage-state.js";
import styles from "./index.module.css";

const TABLES_SIDEBAR_SIZE_STORAGE_KEY = "jazz.inspector.dataExplorer.tablesSidebarSize";
const TABLES_SIDEBAR_DEFAULT_SIZE = 10;
const TABLES_SIDEBAR_MIN_SIZE = 7;
const TABLES_SIDEBAR_MAX_SIZE = 30;

interface DataExplorerOutletContext {
  isTablesPanelOpen: boolean;
}

function isTablesSidebarSize(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isFinite(value) &&
    value >= TABLES_SIDEBAR_MIN_SIZE &&
    value <= TABLES_SIDEBAR_MAX_SIZE
  );
}

interface TablesSidebarProps {
  tableNames: string[];
  selectedTableName?: string;
  runtime: InspectorRuntime;
  queryPropagation: QueryPropagation;
  onQueryPropagationChange: (value: QueryPropagation) => void;
}

function TablesSidebar({
  tableNames,
  selectedTableName,
  runtime,
  queryPropagation,
  onQueryPropagationChange,
}: TablesSidebarProps) {
  return (
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
                onQueryPropagationChange(event.target.checked ? "local-only" : "full");
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
              className={`${styles.tableLink} ${
                selectedTableName === tableName ? styles.tableLinkActive : ""
              }`}
              aria-label={`View ${tableName} data`}
            >
              {tableName}
            </NavLink>
          </li>
        ))}
      </ul>
    </aside>
  );
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
  const [tablesSidebarSize, setTablesSidebarSize] = useLocalStorageState(
    TABLES_SIDEBAR_SIZE_STORAGE_KEY,
    TABLES_SIDEBAR_DEFAULT_SIZE,
    { isValid: isTablesSidebarSize },
  );

  return (
    <Group
      key={isTablesPanelOpen ? "tables-panel-open" : "tables-panel-closed"}
      className={styles.layout}
      orientation="horizontal"
      onLayoutChanged={(layout) => {
        const nextTablesSidebarSize = layout["tables-panel"];
        if (isTablesSidebarSize(nextTablesSidebarSize)) {
          setTablesSidebarSize(nextTablesSidebarSize);
        }
      }}
    >
      {isTablesPanelOpen ? (
        <>
          <Panel
            id="tables-panel"
            className={styles.sidebarPanel}
            defaultSize={`${tablesSidebarSize}%`}
            minSize={`${TABLES_SIDEBAR_MIN_SIZE}%`}
            maxSize={`${TABLES_SIDEBAR_MAX_SIZE}%`}
          >
            <TablesSidebar
              tableNames={tableNames}
              selectedTableName={table}
              runtime={runtime}
              queryPropagation={queryPropagation}
              onQueryPropagationChange={setQueryPropagation}
            />
          </Panel>
          <Separator className={styles.resizeHandle} />
        </>
      ) : null}
      <Panel
        id="data-explorer-content"
        className={styles.contentPanel}
        defaultSize={isTablesPanelOpen ? undefined : "100%"}
        minSize={isTablesPanelOpen ? "40%" : "100%"}
      >
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
