import { NavLink, Outlet } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { useStandaloneContext } from "../../contexts/standalone-context.js";
import {
  formatSchemaHashOptionLabel,
  type SchemaHashInfo,
} from "../../utility/schema-hash-display.js";
import { requestCloseOverlay } from "../../utility/overlay-settings.js";
import { Tooltip } from "../tooltip/Tooltip.js";
import styles from "./index.module.css";

function CloseIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      aria-hidden="true"
    >
      <path d="M6 6l12 12M18 6 6 18" />
    </svg>
  );
}

export function InspectorLayout() {
  const { runtime } = useDevtoolsContext();
  const isOverlay = runtime === "overlay";
  const standaloneContext = useStandaloneContext();

  return (
    <main className={styles.root}>
      <header className={styles.topBar}>
        <nav className={styles.tabBar} aria-label="Inspector sections">
          <NavLink
            to="/data-explorer"
            className={({ isActive }) =>
              `${styles.tabLink} ${isActive ? styles.tabLinkActive : ""}`
            }
          >
            Data Explorer
          </NavLink>
          <NavLink
            to="/live-query"
            className={({ isActive }) =>
              `${styles.tabLink} ${isActive ? styles.tabLinkActive : ""}`
            }
          >
            Live Query
          </NavLink>
          {isOverlay ? (
            <NavLink
              to="/settings"
              className={({ isActive }) =>
                `${styles.tabLink} ${isActive ? styles.tabLinkActive : ""}`
              }
            >
              Settings
            </NavLink>
          ) : null}
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
          {isOverlay ? (
            <Tooltip label="Close (Esc)">
              <button
                type="button"
                onClick={requestCloseOverlay}
                className={styles.iconButton}
                aria-label="Close inspector"
              >
                <CloseIcon />
              </button>
            </Tooltip>
          ) : null}
        </div>
      </header>
      <section className={styles.content}>
        <Outlet context={{ isTablesPanelOpen: true }} />
      </section>
    </main>
  );
}

interface SchemaHashesSelectProps {
  schemaHashes: SchemaHashInfo[];
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
        {schemaHashes.map((schema) => (
          <option key={schema.hash} value={schema.hash} title={schema.hash}>
            {formatSchemaHashOptionLabel(schema)}
          </option>
        ))}
      </select>
    </label>
  );
}
