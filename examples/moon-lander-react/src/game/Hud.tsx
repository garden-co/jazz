import { COLOURS, type PlayerMode } from "./constants.js";

interface HudProps {
  mode: PlayerMode;
  positionX: number;
  positionY: number;
  velocityX: number;
  velocityY: number;
  fuel: number;
  landerX: number;
  requiredFuelType: string;
  inventory: string[];
}

export function Hud({
  mode,
  positionX,
  positionY,
  velocityX,
  velocityY,
  fuel,
  landerX,
  requiredFuelType,
  inventory,
}: HudProps) {
  return (
    <>
      {/* Debug overlay */}
      <div
        style={{
          position: "absolute",
          top: 12,
          left: 12,
          fontFamily: "monospace",
          fontSize: 13,
          color: COLOURS.cyan,
          background: "rgba(10, 10, 15, 0.7)",
          padding: "8px 12px",
          borderRadius: 4,
          lineHeight: 1.6,
          pointerEvents: "none",
        }}
      >
        <div>
          mode: <span style={{ color: COLOURS.pink }}>{mode}</span>
        </div>
        <div>
          pos: {Math.floor(positionX)}, {Math.floor(positionY)}
        </div>
        <div>
          vel: {velocityX.toFixed(1)}, {velocityY.toFixed(1)}
        </div>
        <div>
          fuel:{" "}
          <span
            style={{ color: fuel > 10 ? COLOURS.green : COLOURS.orange }}
          >
            {Math.round(fuel)}
          </span>
        </div>
        <div>
          need:{" "}
          <span style={{ color: inventory.includes(requiredFuelType) ? COLOURS.green : COLOURS.orange }}>
            {requiredFuelType}
          </span>
          {inventory.includes(requiredFuelType) ? " ✓" : ""}
        </div>
        {inventory.length > 0 && (
          <div>
            bag: {inventory.join(", ")}
          </div>
        )}
        {mode === "walking" && (
          <div>
            lander: {Math.floor(landerX)} (dist:{" "}
            {Math.floor(Math.abs(positionX - landerX))})
          </div>
        )}
      </div>

      {/* Controls hint */}
      <div
        style={{
          position: "absolute",
          bottom: 12,
          left: "50%",
          transform: "translateX(-50%)",
          fontFamily: "monospace",
          fontSize: 12,
          color: "rgba(255, 255, 255, 0.4)",
          pointerEvents: "none",
        }}
      >
        {mode === "descending" && "Arrow keys / WASD — thrust"}
        {mode === "landed" && "E — exit lander | Enter — chat"}
        {mode === "in_lander" && (fuel >= 100 ? "Space — launch | E — exit | Enter — chat" : "E — exit lander | Enter — chat")}
        {mode === "walking" && "A/D — walk | E — enter lander | Enter — chat"}
      </div>
    </>
  );
}
