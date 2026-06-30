import type { ReactNode } from "react";
import { useDevtoolsContext } from "../../contexts/devtools-context.js";
import { useLocalStorageState } from "../../utility/use-local-storage-state.js";
import { OVERLAY_HIDE_LAUNCHER_STORAGE_KEY, isBoolean } from "../../utility/overlay-settings.js";
import styles from "./index.module.css";

// macOS spells the shortcut with glyphs; everything else uses words. Computed
// once at module load — the host platform doesn't change within a session.
const IS_APPLE =
  typeof navigator !== "undefined" && /Mac|iPhone|iPad|iPod/.test(navigator.platform);
const OPEN_SHORTCUT_KEYS = IS_APPLE ? ["⌥", "⇧", "J"] : ["Alt", "Shift", "J"];

function KeyCombo({ keys }: { keys: string[] }) {
  return (
    <span className={styles.shortcut} aria-label={keys.join(" ")}>
      {keys.map((key, index) => (
        // Keys are a fixed, ordered set; index keys are stable here.
        <kbd key={index} className={styles.key}>
          {key}
        </kbd>
      ))}
    </span>
  );
}

interface Shortcut {
  keys: string[];
  label: string;
  overlayOnly?: boolean;
}

// Mirrors the bindings wired in the overlay loader (Alt+Shift+J / Esc) and the
// data grid (double-click / Del / Shift-click). Keep in sync if those change.
const SHORTCUTS: Shortcut[] = [
  { keys: OPEN_SHORTCUT_KEYS, label: "Open or close the inspector", overlayOnly: true },
  { keys: ["Esc"], label: "Close the inspector", overlayOnly: true },
  { keys: ["Double-click"], label: "Edit a cell" },
  { keys: ["Del"], label: "Delete the focused row" },
  { keys: ["Shift", "Click"], label: "Select a range of rows" },
];

interface ToggleRowProps {
  id: string;
  label: string;
  description: ReactNode;
  checked: boolean;
  onChange: (checked: boolean) => void;
}

function ToggleRow({ id, label, description, checked, onChange }: ToggleRowProps) {
  const descriptionId = `${id}-description`;
  return (
    <div className={styles.row}>
      <div className={styles.rowText}>
        <label htmlFor={id} className={styles.rowLabel}>
          {label}
        </label>
        <p id={descriptionId} className={styles.rowDescription}>
          {description}
        </p>
      </div>
      <label className={styles.switch}>
        <input
          id={id}
          type="checkbox"
          role="switch"
          className={styles.switchInput}
          checked={checked}
          aria-describedby={descriptionId}
          onChange={(event) => onChange(event.target.checked)}
        />
        <span className={styles.switchTrack} aria-hidden="true">
          <span className={styles.switchThumb} />
        </span>
      </label>
    </div>
  );
}

export function SettingsPage() {
  const { isOverlay } = useDevtoolsContext();
  const [hideLauncher, setHideLauncher] = useLocalStorageState<boolean>(
    OVERLAY_HIDE_LAUNCHER_STORAGE_KEY,
    false,
    { isValid: isBoolean },
  );

  return (
    <div className={styles.scroll}>
      <div className={styles.page}>
        <h1 className={styles.heading}>Settings</h1>

        {isOverlay ? (
          <section className={styles.section} aria-labelledby="settings-launcher">
            <h2 id="settings-launcher" className={styles.sectionTitle}>
              Launcher
            </h2>
            <div className={styles.card}>
              <ToggleRow
                id="hide-launcher"
                label="Hide the launcher button"
                description={
                  <>
                    Keep the inspector out of the way. Open it any time with{" "}
                    <KeyCombo keys={OPEN_SHORTCUT_KEYS} />.
                  </>
                }
                checked={hideLauncher}
                onChange={setHideLauncher}
              />
            </div>
          </section>
        ) : (
          <p className={styles.emptyNote}>No settings are available in this view.</p>
        )}

        <section className={styles.section} aria-labelledby="settings-shortcuts">
          <h2 id="settings-shortcuts" className={styles.sectionTitle}>
            Keyboard shortcuts
          </h2>
          <div className={styles.card}>
            <dl className={styles.shortcutList}>
              {SHORTCUTS.filter((shortcut) => isOverlay || !shortcut.overlayOnly).map(
                (shortcut) => (
                  <div className={styles.shortcutRow} key={shortcut.label}>
                    <dt className={styles.shortcutLabel}>{shortcut.label}</dt>
                    <dd className={styles.shortcutKeys}>
                      <KeyCombo keys={shortcut.keys} />
                    </dd>
                  </div>
                ),
              )}
            </dl>
          </div>
        </section>
      </div>
    </div>
  );
}
