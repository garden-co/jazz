import { useEffect } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { useStandaloneContext } from "../../contexts/standalone-context.js";
import {
  formatSchemaHashOptionLabel,
  type SchemaHashInfo,
} from "../../utility/schema-hash-display.js";
import {
  DETACH_SHORTCUT_TOOLTIP,
  OVERLAY_ROUTE_MESSAGE_TYPE,
  OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY,
  isBoolean,
  isDetachedInspector,
  requestCloseOverlay,
  requestDetachOverlay,
  setOverlayActiveRoute,
} from "../../utility/overlay-settings.js";
import { useLocalStorageState } from "../../utility/use-local-storage-state.js";
import { Tooltip } from "../tooltip/Tooltip.js";
import styles from "./index.module.css";

const TABLES_PANEL_OPEN_STORAGE_KEY = "jazz.inspector.dataExplorer.tablesPanelOpen";

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

function PictureInPictureIcon() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M2 10h6V4" />
      <path d="m2 4 6 6" />
      <path d="M21 10V7a2 2 0 0 0-2-2h-7" />
      <path d="M3 14v2a2 2 0 0 0 2 2h3" />
      <rect x="12" y="14" width="10" height="7" rx="1" />
    </svg>
  );
}

export function InspectorLayout() {
  const { runtime } = useDevtoolsContext();
  const isOverlay = runtime === "overlay";
  const isDetached = isOverlay && isDetachedInspector();
  const standaloneContext = useStandaloneContext();
  const location = useLocation();
  const navigate = useNavigate();
  const [isTablesPanelOpen, setIsTablesPanelOpen] = useLocalStorageState(
    TABLES_PANEL_OPEN_STORAGE_KEY,
    true,
  );
  const [showDetachButton, setShowDetachButton] = useLocalStorageState<boolean>(
    OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY,
    true,
    { isValid: isBoolean },
  );

  const isDataExplorerRoute = location.pathname.startsWith("/data-explorer");
  const activeRoute = `${location.pathname}${location.search}`;

  useEffect(() => {
    if (!isOverlay) return;
    setOverlayActiveRoute(activeRoute);

    if (isDetached) return;

    const onKeyDown = (event: KeyboardEvent) => {
      if (!event.altKey || !event.shiftKey || event.code !== "KeyD") return;
      event.preventDefault();
      requestDetachOverlay(activeRoute);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [activeRoute, isDetached, isOverlay]);

  useEffect(() => {
    if (!isOverlay || isDetached) return;
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== window.location.origin || event.source !== window.parent) return;
      const data = event.data as { type?: unknown; route?: unknown } | null;
      if (
        data?.type === OVERLAY_ROUTE_MESSAGE_TYPE &&
        typeof data.route === "string" &&
        data.route.startsWith("/")
      ) {
        navigate(data.route);
      }
    };
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [isDetached, isOverlay, navigate]);

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
            Subscriptions
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
          {isOverlay && !isDetached ? (
            <>
              {showDetachButton ? (
                <Tooltip label={DETACH_SHORTCUT_TOOLTIP}>
                  <button
                    type="button"
                    onClick={() => requestDetachOverlay(activeRoute)}
                    className={styles.iconButton}
                    aria-label="Open inspector in separate window"
                  >
                    <PictureInPictureIcon />
                  </button>
                </Tooltip>
              ) : null}
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
            </>
          ) : null}
        </div>
      </header>
      <section className={styles.content}>
        <Outlet context={{ isTablesPanelOpen, showDetachButton, setShowDetachButton }} />
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
