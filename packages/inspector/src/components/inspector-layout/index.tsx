import { NavLink, Outlet } from "react-router";
import { useStandaloneContext } from "../../contexts/standalone-context.js";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import styles from "./index.module.css";

export function InspectorLayout() {
  const standaloneContext = useStandaloneContext();
  const { runtime } = useDevtoolsContext();

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
          {runtime === "extension" ? (
            <NavLink
              to="/live-query"
              className={({ isActive }) =>
                `${styles.tabLink} ${isActive ? styles.tabLinkActive : ""}`
              }
            >
              Live Query
            </NavLink>
          ) : null}
        </nav>
        {standaloneContext ? (
          <div className={styles.topBarActions}>
            <SchemaHashesSelect
              schemaHashes={standaloneContext.schemaHashes}
              selectedSchemaHash={standaloneContext.selectedSchemaHash}
              onSelectSchema={standaloneContext.onSelectSchema}
              isSwitchingSchema={standaloneContext.isSwitchingSchema}
            />
            <button
              type="button"
              onClick={standaloneContext.onReset}
              className={styles.resetButton}
            >
              Reset connection
            </button>
          </div>
        ) : null}
      </header>
      <section className={styles.content}>
        <Outlet />
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
