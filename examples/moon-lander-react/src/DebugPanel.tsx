import { useEffect, useState } from "react";
import type { PlayerInit } from "../schema/app";
import styles from "./DebugPanel.module.css";
import { FUEL_TYPES } from "./game/constants";
import type { DepositState } from "./sync/useDeposits";

// ---------------------------------------------------------------------------
// Debug panel — self-contained: manages own open/closed state via 'j' key
// ---------------------------------------------------------------------------

const DEBUG_STORAGE_KEY = "moonlander-debug-open";

export interface DebugPanelProps {
  depositState: DepositState;
  remotePlayerCount: number;
  chatMessageCount: number;
  gameState: React.RefObject<PlayerInit | null>;
}

export function DebugPanel({
  depositState,
  remotePlayerCount,
  chatMessageCount,
  gameState,
}: DebugPanelProps) {
  const [open, setOpen] = useState(() => {
    try {
      return localStorage.getItem(DEBUG_STORAGE_KEY) === "true";
    } catch {
      return false;
    }
  });

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.code === "KeyJ" && !e.ctrlKey && !e.metaKey && !e.altKey) {
        setOpen((prev) => {
          const next = !prev;
          try {
            localStorage.setItem(DEBUG_STORAGE_KEY, String(next));
          } catch {
            /* ignore */
          }
          return next;
        });
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  if (!open) return null;

  const { perTypeCounts, perTypeLimits, myCollectedCount, localPlayerRows } =
    depositState;
  const totalSubscriptionItems =
    perTypeCounts.reduce((a, b) => a + b, 0) +
    myCollectedCount +
    remotePlayerCount +
    localPlayerRows.length +
    chatMessageCount;

  const gs = gameState.current;

  return (
    <div className={styles.debugPanel}>
      <div className={styles.debugHeader}>debug (j to close)</div>
      <div className={styles.debugSectionFirst}>subscriptions</div>
      <div>total items: {totalSubscriptionItems}</div>
      <div className={styles.debugSection}>deposits (uncollected per type)</div>
      {FUEL_TYPES.map((ft, i) => (
        <div key={ft}>
          {ft}: {perTypeCounts[i]} / {perTypeLimits[i]}
        </div>
      ))}
      <div>my collected: {myCollectedCount}</div>
      <div className={styles.debugSpacer}>
        TOTAL rows: {depositState.debugTotalDeposits}
      </div>
      <div className={styles.debugSection}>players</div>
      <div>remote: {remotePlayerCount}</div>
      <div>local rows: {localPlayerRows.length}</div>
      <div className={styles.debugSpacer}>
        chat messages: {chatMessageCount}
      </div>
      <div className={styles.debugSection}>game state</div>
      <div>mode: {gs?.mode ?? "?"}</div>
      <div>
        pos: {Math.round(gs?.positionX ?? 0)}, {Math.round(gs?.positionY ?? 0)}
      </div>
      <div>
        vel: {Math.round(gs?.velocityX ?? 0)}, {Math.round(gs?.velocityY ?? 0)}
      </div>
    </div>
  );
}
