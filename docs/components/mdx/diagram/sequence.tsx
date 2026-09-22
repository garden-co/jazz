import type { ReactNode } from "react";

import { DiagramFrame } from "./frame";
import { DiagramStyles } from "./styles";
import { layoutSequence, type Participant, type SequenceStep } from "./sequence-layout";

export type { Participant, SequenceStep } from "./sequence-layout";

export type SequenceProps = {
  eyebrow: string;
  description: ReactNode;
  participants: Participant[];
  steps: SequenceStep[];
};

const EDGE = "var(--diagram-edge, #9ca3af)";
const BG = "var(--diagram-bg, #ffffff)";
const FG = "var(--diagram-fg, #18181b)";
const MUTED = "var(--diagram-muted, #71717a)";
const CARD = "var(--diagram-card, #ffffff)";
const CARD_MUTED = "var(--diagram-card-muted, #f4f4f5)";
const BORDER = "var(--diagram-border, #e4e4e7)";

// Matches the kit's node typography (.dg-node-title / .dg-node) so actor boxes
// read like the TierSync tier headers and Lens data/lens labels.
const TITLE_PX = 12;
const LABEL_PX = 12;
const LINE_H = 16;

// Arrowhead polygon at `tip`, pointing left or right.
function Arrow({ x, y, dir }: { x: number; y: number; dir: "left" | "right" }) {
  const s = dir === "right" ? -1 : 1;
  return <polygon points={`${x},${y} ${x + s * 8},${y - 4} ${x + s * 8},${y + 4}`} fill={EDGE} />;
}

// One <text> with a centred stack of <tspan> lines.
function Lines({
  x,
  y,
  lines,
  fill,
  anchor = "middle",
  italic,
  weight,
  mask,
}: {
  x: number;
  y: number;
  lines: string[];
  fill: string;
  anchor?: "start" | "middle" | "end";
  italic?: boolean;
  weight?: number;
  // A background-coloured rect sized exactly to the text — its only job is to
  // break the dashed lifeline behind the label so it stays readable.
  mask?: boolean;
}) {
  const startDy = (-(lines.length - 1) * LINE_H) / 2;
  const maxLen = lines.reduce((m, l) => Math.max(m, l.length), 0);
  const pw = maxLen * 6.8;
  const ph = lines.length * LINE_H;
  const px = anchor === "start" ? x : anchor === "end" ? x - pw : x - pw / 2;
  return (
    <>
      {mask && lines.some((l) => l !== "") ? (
        <rect x={px} y={y - ph / 2} width={pw} height={ph} fill={BG} />
      ) : null}
      <text
        x={x}
        y={y}
        fill={fill}
        fontSize={weight ? TITLE_PX : LABEL_PX}
        fontWeight={weight}
        fontStyle={italic ? "italic" : undefined}
        textAnchor={anchor}
        dominantBaseline="central"
      >
        {lines.map((ln, k) => (
          <tspan key={k} x={x} dy={k === 0 ? startDy : LINE_H}>
            {ln}
          </tspan>
        ))}
      </text>
    </>
  );
}

export function Sequence({ eyebrow, description, participants, steps }: SequenceProps) {
  const L = layoutSequence(participants, steps);

  return (
    <DiagramFrame eyebrow={eyebrow} description={description} responsive>
      <DiagramStyles />
      <div className="diagram-host" style={{ width: "100%", overflowX: "auto" }}>
        <svg
          viewBox={`0 0 ${L.width} ${L.height}`}
          width="100%"
          style={{ maxWidth: L.width, height: "auto", display: "block", margin: "0 auto" }}
          role="img"
        >
          {/* lifelines */}
          {participants.map((p) => {
            const a = L.actors[p.id];
            return (
              <line
                key={`life-${p.id}`}
                x1={a.cx}
                y1={a.lifeTop}
                x2={a.cx}
                y2={L.lifeBottom}
                stroke={EDGE}
                strokeWidth={1.5}
                strokeDasharray="4 4"
              />
            );
          })}

          {/* steps */}
          {steps.map((s, i) => {
            const y = L.stepY[i];
            const lines = L.stepLines[i];
            if (s.kind === "note") {
              const ids = Array.isArray(s.over) ? s.over : [s.over];
              const xs = ids.map((id) => L.actors[id].cx);
              const cx = (Math.min(...xs) + Math.max(...xs)) / 2;
              const maxLen = lines.reduce((m, l) => Math.max(m, l.length), 0);
              const w = Math.max(
                Math.min(maxLen, 18) * 8 + 24,
                Math.max(...xs) - Math.min(...xs) + 80,
              );
              const h = lines.length * LINE_H + 14;
              return (
                <g key={`step-${i}`}>
                  <rect
                    x={cx - w / 2}
                    y={y - h / 2}
                    width={w}
                    height={h}
                    rx={4}
                    fill={CARD_MUTED}
                    stroke={BORDER}
                  />
                  <Lines x={cx} y={y} lines={lines} fill={MUTED} italic />
                </g>
              );
            }
            const from = L.actors[s.from];
            const to = L.actors[s.to];
            const dashed = s.line === "dashed";
            if (s.from === s.to) {
              const x0 = from.cx;
              const w = 46;
              return (
                <g key={`step-${i}`}>
                  <path
                    d={`M ${x0} ${y - 7} L ${x0 + w} ${y - 7} L ${x0 + w} ${y + 9} L ${x0} ${y + 9}`}
                    fill="none"
                    stroke={EDGE}
                    strokeWidth={1.5}
                    strokeDasharray={dashed ? "4 3" : undefined}
                  />
                  <Arrow x={x0} y={y + 9} dir="left" />
                  <Lines x={x0 + w + 8} y={y + 1} lines={lines} fill={MUTED} anchor="start" mask />
                </g>
              );
            }
            const rightward = to.cx > from.cx;
            // Actor boxes are drawn last (so they sit above lifelines), which
            // means a message at the same y as a creation step would normally
            // have its tip swallowed by the new actor box. When the endpoint
            // is being created at this step, land on the box edge instead of
            // the lifeline centre.
            const x1 =
              from.createAtStep === i
                ? from.cx + (rightward ? from.boxW / 2 : -from.boxW / 2)
                : from.cx;
            const x2 =
              to.createAtStep === i ? to.cx + (rightward ? -to.boxW / 2 : to.boxW / 2) : to.cx;
            const tipX = x2 + (rightward ? -1 : 1);
            const labelY = y - 12 - ((lines.length - 1) * LINE_H) / 2;
            return (
              <g key={`step-${i}`}>
                <line
                  x1={x1}
                  y1={y}
                  x2={x2}
                  y2={y}
                  stroke={EDGE}
                  strokeWidth={1.5}
                  strokeDasharray={dashed ? "4 3" : undefined}
                />
                <Arrow x={tipX} y={y} dir={rightward ? "right" : "left"} />
                <Lines x={(x1 + x2) / 2} y={labelY} lines={lines} fill={MUTED} mask />
              </g>
            );
          })}

          {/* actor boxes (last, so they sit above lifelines/messages) */}
          {participants.map((p) => {
            const a = L.actors[p.id];
            return (
              <g key={`actor-${p.id}`}>
                <rect
                  x={a.cx - a.boxW / 2}
                  y={a.boxY}
                  width={a.boxW}
                  height={a.boxH}
                  rx={8}
                  fill={CARD}
                  stroke={BORDER}
                  strokeWidth={2}
                />
                <Lines x={a.cx} y={a.boxY + a.boxH / 2} lines={[p.label]} fill={FG} weight={600} />
              </g>
            );
          })}
        </svg>
      </div>
    </DiagramFrame>
  );
}
