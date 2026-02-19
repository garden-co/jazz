import { useEffect, useRef } from "react";
import type { Player } from "../../schema/app";
import { COLOURS, type FuelType, type PlayerMode } from "./constants";
import styles from "./Hud.module.css";
import { getDepositSprite } from "./sprites";

interface HudProps {
  mode: PlayerMode;
  fuel: number;
  requiredFuelType: FuelType;
  inventory: string[];
  remotePlayers: Player[];
  localPlayerName: string;
  localPlayerColor: string;
}

// Renders the actual deposit sprite into a tiny canvas
function DepositIcon({ type, dim }: { type: string; dim?: boolean }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, 12, 12);
    if (dim) ctx.globalAlpha = 0.3;
    else ctx.globalAlpha = 1;
    ctx.imageSmoothingEnabled = false;
    const sprite = getDepositSprite(type as FuelType);
    ctx.drawImage(sprite, 0, 0, 12, 12);
  }, [type, dim]);

  return (
    <canvas
      ref={canvasRef}
      width={12}
      height={12}
      className={styles.depositIcon}
    />
  );
}

// Fuel bar: chunky segmented gauge
function FuelBar({ fuel }: { fuel: number }) {
  const segments = 10;
  const filled = Math.ceil((fuel / 100) * segments);
  const barColour =
    fuel > 30 ? COLOURS.green : fuel > 10 ? COLOURS.orange : "#ff3333";

  return (
    <div
      className={styles.fuelBar}
      style={{ "--bar-colour": barColour } as React.CSSProperties}
    >
      <span className={styles.fuelLabel}>fuel</span>
      {Array.from({ length: segments }, (_, i) => (
        <span
          key={i}
          className={i < filled ? styles.segmentFilled : styles.segment}
        />
      ))}
      <span className={styles.fuelValue}>{Math.round(fuel)}</span>
    </div>
  );
}

const HIDDEN_MODES = new Set<PlayerMode>(["start", "launched", "crashed"]);

export function Hud({
  mode,
  fuel,
  requiredFuelType,
  inventory,
  remotePlayers,
  localPlayerName,
  localPlayerColor,
}: HudProps) {
  if (HIDDEN_MODES.has(mode)) return null;

  const hasRequired = inventory.includes(requiredFuelType);

  return (
    <>
      {/* Left panel */}
      <div className={styles.panelLeft}>
        <FuelBar fuel={fuel} />
        <div
          className={styles.needRow}
          style={
            {
              "--status-colour": hasRequired ? COLOURS.green : COLOURS.orange,
            } as React.CSSProperties
          }
        >
          <span className={styles.label}>need</span>
          <DepositIcon type={requiredFuelType} dim={!hasRequired} />
          <span className={styles.needStatus}>{requiredFuelType}</span>
          {hasRequired && <span className={styles.checkmark}>{"\u2713"}</span>}
        </div>
        {inventory.length > 0 && (
          <div className={styles.bagRow}>
            <span className={styles.label}>bag</span>
            {inventory.map((ft, i) => (
              <DepositIcon key={i} type={ft} />
            ))}
          </div>
        )}
      </div>

      {/* Right panel */}
      {remotePlayers.length > 0 && (
        <div className={styles.panelRight}>
          <div className={styles.playersHeader}>players</div>
          {/* Local player */}
          <div
            className={styles.playerRow}
            style={
              {
                "--player-color": localPlayerColor || COLOURS.cyan,
              } as React.CSSProperties
            }
          >
            <span className={styles.playerDot} />
            <span className={styles.playerName}>
              {localPlayerName || "you"}
            </span>
            <DepositIcon type={requiredFuelType} />
          </div>
          {/* Remote players */}
          {remotePlayers.map((rp) => (
            <div
              key={rp.id}
              className={styles.playerRow}
              style={
                {
                  "--player-color": rp.color || COLOURS.cyan,
                } as React.CSSProperties
              }
            >
              <span className={styles.playerDot} />
              <span className={styles.playerName}>{rp.name}</span>
              {rp.requiredFuelType && (
                <DepositIcon type={rp.requiredFuelType} />
              )}
            </div>
          ))}
        </div>
      )}

      {/* Controls hint */}
      <div className={styles.controlsHint}>
        {mode === "descending" && "arrows / WASD \u2014 thrust"}
        {mode === "landed" && "E \u2014 exit lander | Enter \u2014 chat"}
        {mode === "in_lander" &&
          (fuel >= 100
            ? "Space \u2014 launch | E \u2014 exit | Enter \u2014 chat"
            : "E \u2014 exit lander | Enter \u2014 chat")}
        {mode === "walking" &&
          "A/D \u2014 walk | E \u2014 enter lander | Enter \u2014 chat"}
      </div>
    </>
  );
}
