import { NavLink, Outlet, useOutletContext } from "react-router";
import { useSchemaCatalog, type SchemaCatalogState } from "./schema-catalog.js";
import { shortHash } from "./schema-analysis.js";
import styles from "./index.module.css";

export function SchemaExplorer() {
  const catalog = useSchemaCatalog();

  return (
    <div className={styles.layout}>
      <header className={styles.header}>
        <div>
          <h2 className={styles.title}>Schema Explorer</h2>
          <p className={styles.description}>
            Inspect the active schema as a relational diagram, compare schema versions, and trace
            compatibility edges across the catalogue.
          </p>
        </div>
        <div className={styles.summary}>
          <div className={styles.summaryItem}>
            <span className={styles.summaryLabel}>Current</span>
            <span className={styles.summaryValue}>
              {catalog.currentSchemaHash ? shortHash(catalog.currentSchemaHash) : "runtime"}
            </span>
          </div>
          <div className={styles.summaryItem}>
            <span className={styles.summaryLabel}>Known schemas</span>
            <span className={styles.summaryValue}>
              {catalog.supportsCatalogue ? catalog.hashes.length : 1}
            </span>
          </div>
          <div className={styles.summaryItem}>
            <span className={styles.summaryLabel}>Migration edges</span>
            <span className={styles.summaryValue}>
              {catalog.supportsCatalogue ? catalog.migrations.length : "n/a"}
            </span>
          </div>
        </div>
      </header>

      <nav className={styles.subnav} aria-label="Schema explorer views">
        <NavLink
          to="/schemas"
          end
          className={({ isActive }) =>
            `${styles.subnavLink} ${isActive ? styles.subnavLinkActive : ""}`
          }
        >
          Single schema
        </NavLink>
        <NavLink
          to="/schemas/compatibility"
          className={({ isActive }) =>
            `${styles.subnavLink} ${isActive ? styles.subnavLinkActive : ""}`
          }
        >
          Compatibility graph
        </NavLink>
        <NavLink
          to="/schemas/compare"
          className={({ isActive }) =>
            `${styles.subnavLink} ${isActive ? styles.subnavLinkActive : ""}`
          }
        >
          Compare
        </NavLink>
      </nav>

      {catalog.error ? (
        <div className={styles.alert} role="alert">
          {catalog.error}
        </div>
      ) : null}

      <div className={styles.content}>
        <Outlet context={catalog satisfies SchemaCatalogState} />
      </div>
    </div>
  );
}

export function useSchemaExplorerContext() {
  return useOutletContext<SchemaCatalogState>();
}
