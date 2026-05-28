import { createContext, useContext, useState } from "react";
import { Link, Outlet, useLocation, useParams } from "@tanstack/react-router";
import { useStandaloneContext } from "../../contexts/standalone-context.js";
import { appRoutes } from "#lib/navigation/appRoutes.ts";
import styles from "./index.module.css";

export interface InspectorLayoutProps {
  children?: React.ReactNode;
}

interface InspectorLayoutContextValue {
  isTablesPanelOpen: boolean;
}

const InspectorLayoutContext = createContext<InspectorLayoutContextValue | null>(null);

export function useInspectorLayoutContext(): InspectorLayoutContextValue {
  const context = useContext(InspectorLayoutContext);
  if (context === null) {
    throw new Error("useInspectorLayoutContext must be used inside InspectorLayout");
  }
  return context;
}

interface TablesPanelIconProps {
  direction: "open" | "close";
}

function TablesPanelIcon({ direction }: TablesPanelIconProps) {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="2.5" y="2.5" width="11" height="11" rx="1.5" />
      <path d="M6 3v10" />
      {direction === "close" ? <path d="M10 6l-2 2 2 2" /> : <path d="M8 6l2 2-2 2" />}
    </svg>
  );
}

export function InspectorLayout({ children }: InspectorLayoutProps) {
  const standaloneContext = useStandaloneContext();
  const location = useLocation();
  const params = useParams({ strict: false });
  const [isTablesPanelOpen, setIsTablesPanelOpen] = useState(true);

  const routeParams = {
    connectionId: params.connectionId ?? "",
    branch: params.branch ?? "",
    schemaHash: params.schemaHash ?? "",
  };
  const isDataExplorerRoute = location.pathname.includes("/data-explorer");

  const onToggleTablesPanel = () => {
    setIsTablesPanelOpen((isOpen) => !isOpen);
  };

  return (
    <main className={styles.root}>
      <header className={styles.topBar}>
        <nav className={styles.tabBar} aria-label="Inspector sections">
          {isDataExplorerRoute ? (
            <button
              type="button"
              onClick={onToggleTablesPanel}
              className={styles.iconButton}
              aria-label={isTablesPanelOpen ? "Collapse tables panel" : "Expand tables panel"}
              aria-pressed={isTablesPanelOpen}
            >
              <TablesPanelIcon direction={isTablesPanelOpen ? "close" : "open"} />
            </button>
          ) : null}
          <Link
            to={appRoutes.dataExplorer}
            params={routeParams}
            className={styles.tabLink}
            activeProps={{ className: styles.tabLinkActive }}
          >
            Data Explorer
          </Link>
          <Link
            to={appRoutes.liveQuery}
            params={routeParams}
            className={styles.tabLink}
            activeProps={{ className: styles.tabLinkActive }}
          >
            Live Query
          </Link>
        </nav>
        <div className={styles.topBarActions}>
          {standaloneContext ? (
            <>
              <SchemaHashesSelect
                schemaHashes={standaloneContext.schemaHashes}
                selectedSchemaHash={standaloneContext.selectedSchemaHash}
                onSelectSchema={standaloneContext.onSelectSchema}
                isSwitchingSchema={standaloneContext.isSwitchingSchema}
              />
              <button
                type="button"
                onClick={standaloneContext.onManageConnections}
                className={styles.resetButton}
              >
                Connections
              </button>
            </>
          ) : null}
        </div>
      </header>
      <section className={styles.content}>
        <InspectorLayoutContext.Provider value={{ isTablesPanelOpen }}>
          {children ?? <Outlet />}
        </InspectorLayoutContext.Provider>
      </section>
    </main>
  );
}

interface SchemaHashesSelectProps {
  schemaHashes: string[];
  selectedSchemaHash: string | null;
  onSelectSchema: (schemaHash: string) => void;
  isSwitchingSchema: boolean;
}

export function SchemaHashesSelect({
  schemaHashes,
  selectedSchemaHash,
  onSelectSchema,
  isSwitchingSchema,
}: SchemaHashesSelectProps) {
  return (
    <label className={styles.schemaSelectLabel}>
      Schema
      <select
        className={styles.schemaSelect}
        value={selectedSchemaHash ?? ""}
        onChange={(event) => onSelectSchema(event.target.value)}
        disabled={isSwitchingSchema || schemaHashes.length === 0}
      >
        {schemaHashes.map((schemaHash) => (
          <option key={schemaHash} value={schemaHash}>
            {schemaHash}
          </option>
        ))}
      </select>
    </label>
  );
}
