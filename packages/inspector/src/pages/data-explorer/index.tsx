import { useEffect, useMemo } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { NavLink, Outlet, useNavigate, useOutletContext, useParams } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
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
}

function TablesSidebar({ tableNames, selectedTableName }: TablesSidebarProps) {
  return (
    <aside className={styles.sidebar}>
      <div className={styles.sidebarHeader}>
        <h2 className={styles.sidebarTitle}>Tables</h2>
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
  const { wasmSchema: schema } = useDevtoolsContext();
  const isTablesPanelOpen =
    useOutletContext<DataExplorerOutletContext | null>()?.isTablesPanelOpen ?? true;
  const { table } = useParams();
  const navigate = useNavigate();

  const tableNames = useMemo(() => Object.keys(schema ?? {}).sort(), [schema]);

  // Land directly on the first table instead of an interstitial picker — opening
  // the explorer to an empty pane is friction every single time. The real empty
  // state below is reserved for a schema with no tables at all.
  useEffect(() => {
    if (!table && tableNames.length > 0) {
      navigate(`/data-explorer/${tableNames[0]}/data`, { replace: true });
    }
  }, [table, tableNames, navigate]);
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
            <TablesSidebar tableNames={tableNames} selectedTableName={table} />
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
          {!table && tableNames.length === 0 ? (
            <section className={styles.emptyState}>
              <h3 className={styles.emptyTitle}>No tables</h3>
              <p className={styles.emptyText}>This schema doesn’t define any tables yet.</p>
            </section>
          ) : null}
          <Outlet />
        </main>
      </Panel>
    </Group>
  );
}
