import { useEffect, useRef } from "react";
import { COLOURS, type FuelType, type PlayerMode } from "./constants.js";
import { getDepositSprite } from "./sprites.js";
import type { RemotePlayerView } from "./types.js";

interface HudProps {
  mode: PlayerMode;
  fuel: number;
  requiredFuelType: FuelType;
  inventory: string[];
  remotePlayers: RemotePlayerView[];
  localPlayerName: string;
  localPlayerColor: string;
}

// Shared panel style: hard pixel border, no rounded corners
const panelStyle: React.CSSProperties = {
  position: "absolute",
  fontFamily: "monospace",
  color: COLOURS.cyan,
  background: "rgba(10, 4, 20, 0.85)",
  border: `1px solid ${COLOURS.pink}`,
  boxShadow: `0 0 8px rgba(255, 0, 255, 0.3), inset 0 0 12px rgba(255, 0, 255, 0.05)`,
  padding: "6px 10px",
  pointerEvents: "none",
  imageRendering: "pixelated",
};

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
      style={{
        display: "inline-block",
        width: 12,
        height: 12,
        verticalAlign: "middle",
        marginLeft: 4,
        imageRendering: "pixelated",
      }}
    />
  );
}

// Fuel bar: chunky segmented gauge
function FuelBar({ fuel }: { fuel: number }) {
  const segments = 10;
  const filled = Math.ceil((fuel / 100) * segments);
  const barColour = fuel > 30 ? COLOURS.green : fuel > 10 ? COLOURS.orange : "#ff3333";

  return (
    <div style={{ display: "flex", gap: 2, alignItems: "center" }}>
      <span
        style={{
          color: COLOURS.pink,
          fontSize: 10,
          marginRight: 4,
          textTransform: "uppercase",
          letterSpacing: 1,
        }}
      >
        fuel
      </span>
      {Array.from({ length: segments }, (_, i) => (
        <span
          key={i}
          style={{
            display: "inline-block",
            width: 6,
            height: 10,
            background: i < filled ? barColour : "rgba(255, 255, 255, 0.08)",
            boxShadow: i < filled ? `0 0 4px ${barColour}` : "none",
          }}
        />
      ))}
      <span style={{ color: barColour, fontSize: 10, marginLeft: 4 }}>{Math.round(fuel)}</span>
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
      <div
        style={{
          ...panelStyle,
          top: 10,
          left: 10,
          fontSize: 11,
          lineHeight: 1.8,
        }}
      >
        <FuelBar fuel={fuel} />
        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
          <span
            style={{
              color: COLOURS.pink,
              textTransform: "uppercase",
              letterSpacing: 1,
              fontSize: 10,
            }}
          >
            need
          </span>
          <DepositIcon type={requiredFuelType} dim={!hasRequired} />
          <span style={{ color: hasRequired ? COLOURS.green : COLOURS.orange, fontSize: 11 }}>
            {requiredFuelType}
          </span>
          {hasRequired && <span style={{ color: COLOURS.green }}>{"\u2713"}</span>}
        </div>
        {inventory.length > 0 && (
          <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
            <span
              style={{
                color: COLOURS.pink,
                textTransform: "uppercase",
                letterSpacing: 1,
                fontSize: 10,
              }}
            >
              bag
            </span>
            {inventory.map((ft, i) => (
              <DepositIcon key={i} type={ft} />
            ))}
          </div>
        )}
      </div>

      {/* Right panel */}
      {remotePlayers.length > 0 && (
        <div
          style={{
            ...panelStyle,
            top: 10,
            right: 10,
            fontSize: 11,
            lineHeight: 1.9,
            minWidth: 90,
          }}
        >
          <div
            style={{
              color: COLOURS.pink,
              fontSize: 10,
              textTransform: "uppercase",
              letterSpacing: 1,
              marginBottom: 2,
            }}
          >
            players
          </div>
          {/* Local player */}
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <span
              style={{
                display: "inline-block",
                width: 6,
                height: 6,
                background: localPlayerColor || COLOURS.cyan,
                boxShadow: `0 0 5px ${localPlayerColor || COLOURS.cyan}`,
              }}
            />
            <span style={{ color: localPlayerColor || COLOURS.cyan }}>
              {localPlayerName || "you"}
            </span>
            <DepositIcon type={requiredFuelType} />
          </div>
          {/* Remote players */}
          {remotePlayers.map((rp) => (
            <div key={rp.id} style={{ display: "flex", alignItems: "center", gap: 4 }}>
              <span
                style={{
                  display: "inline-block",
                  width: 6,
                  height: 6,
                  background: rp.color || COLOURS.cyan,
                  boxShadow: `0 0 5px ${rp.color || COLOURS.cyan}`,
                }}
              />
              <span style={{ color: rp.color || COLOURS.cyan }}>{rp.name}</span>
              {rp.requiredFuelType && <DepositIcon type={rp.requiredFuelType} />}
            </div>
          ))}
        </div>
      )}

      {/* Controls hint */}
      <div
        style={{
          position: "absolute",
          bottom: 10,
          left: "50%",
          transform: "translateX(-50%)",
          fontFamily: "monospace",
          fontSize: 11,
          color: "rgba(255, 0, 255, 0.35)",
          textTransform: "uppercase",
          letterSpacing: 2,
          pointerEvents: "none",
        }}
      >
        {mode === "descending" && "arrows / WASD \u2014 thrust"}
        {mode === "landed" && "E \u2014 exit lander | Enter \u2014 chat"}
        {mode === "in_lander" &&
          (fuel >= 100
            ? "Space \u2014 launch | E \u2014 exit | Enter \u2014 chat"
            : "E \u2014 exit lander | Enter \u2014 chat")}
        {mode === "walking" && "A/D \u2014 walk | E \u2014 enter lander | Enter \u2014 chat"}
      </div>
    </>
  );
}
