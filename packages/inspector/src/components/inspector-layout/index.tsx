import { NavLink, Outlet } from "react-router";
import { useConfigReset } from "../../contexts/config-reset-context.js";
import styles from "./index.module.css";

export function InspectorLayout() {
  const configReset = useConfigReset();

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
        </nav>
        {configReset ? (
          <button type="button" onClick={configReset.onReset} className={styles.resetButton}>
            Reset connection
          </button>
        ) : null}
      </header>
      <section className={styles.content}>
        <Outlet />
      </section>
    </main>
  );
}
