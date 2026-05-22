// Pure analytic layout for sequence diagrams — no DOM. Coordinates only; the
// component draws. Minimal feature set (participants + optional create-at-step,
// messages, notes) — a discriminated union so kinds are additive later.

export type Participant = { id: string; label: string; createAtStep?: number };

export type SequenceStep =
  | {
      kind: "message";
      from: string;
      to: string;
      text: string;
      line?: "solid" | "dashed";
    }
  | { kind: "note"; over: string | string[]; text: string };

export type Actor = {
  cx: number;
  boxW: number;
  boxH: number;
  boxY: number;
  lifeTop: number;
  // The step at which this actor's box drops in (undefined for actors that
  // sit in the header row). The renderer needs this to land a creation
  // message on the box edge — without it the arrowhead would aim at the
  // lifeline centre and disappear behind the actor box.
  createAtStep?: number;
};

export type SequenceLayout = {
  width: number;
  height: number;
  lifeBottom: number;
  stepY: number[];
  stepLines: string[][];
  actors: Record<string, Actor>;
};

const CHAR_W = 8;
const LABEL_PAD = 7;
const MIN_BOX_W = 60;
const BOX_H = 34;
const COL_GAP = 60;
const MARGIN_X = 16;
const HEADER_TOP = 8;
const STEP_TOP_GAP = 24;
const STEP_H = 48;
const BOTTOM = 24;
const SELF_W = 46;
const MSG_CHAR = 7;
// A single label must never rescale the whole diagram: past this width it
// wraps onto more lines (growing its row's height), it never widens the canvas.
const MAX_LABEL_W = 130;
const LINE_H = 16;
const MAX_LABEL_CHARS = Math.max(1, Math.floor(MAX_LABEL_W / MSG_CHAR));

// Greedy word wrap. Never splits a single token: an unbreakable word longer
// than the budget gets its own (over-long) line rather than being cut.
export function wrapText(text: string, maxChars: number): string[] {
  if (text === "") return [""];
  const lines: string[] = [];
  let line = "";
  for (const word of text.split(" ")) {
    if (line === "") {
      line = word;
    } else if (line.length + 1 + word.length <= maxChars) {
      line += ` ${word}`;
    } else {
      lines.push(line);
      line = word;
    }
  }
  lines.push(line);
  return lines;
}

export function layoutSequence(participants: Participant[], steps: SequenceStep[]): SequenceLayout {
  const boxW = participants.map((p) =>
    Math.max(MIN_BOX_W, p.label.length * CHAR_W + LABEL_PAD * 2),
  );

  const cx: number[] = [];
  for (let i = 0; i < participants.length; i++) {
    cx[i] = i === 0 ? MARGIN_X + boxW[0] / 2 : cx[i - 1] + boxW[i - 1] / 2 + COL_GAP + boxW[i] / 2;
  }
  const idx: Record<string, number> = {};
  participants.forEach((p, i) => {
    idx[p.id] = i;
  });

  // Wrap every step's label up front; the longest line drives width, the line
  // count drives that row's height.
  const stepLines = steps.map((s) => wrapText(s.text, MAX_LABEL_CHARS));
  const longestLine = (lines: string[]) => lines.reduce((m, l) => Math.max(m, l.length), 0);
  // Width contribution of a label is capped at the wrap budget so even an
  // unbreakable token can't balloon the canvas (it overflows its slot instead).
  const labelW = (lines: string[]) => Math.min(longestLine(lines), MAX_LABEL_CHARS) * MSG_CHAR;

  // Notes and self-message labels can extend past the actor columns; bound the
  // real content extent so nothing clips, then offset so the leftmost sits at
  // the margin.
  let minX = MARGIN_X;
  let maxX = cx[cx.length - 1] + boxW[boxW.length - 1] / 2;
  steps.forEach((s, i) => {
    const lines = stepLines[i];
    if (s.kind === "note") {
      const ids = Array.isArray(s.over) ? s.over : [s.over];
      const xs = ids.map((id) => cx[idx[id]]);
      const c = (Math.min(...xs) + Math.max(...xs)) / 2;
      const w = Math.max(
        Math.min(longestLine(lines), MAX_LABEL_CHARS) * CHAR_W + 24,
        Math.max(...xs) - Math.min(...xs) + 80,
      );
      minX = Math.min(minX, c - w / 2);
      maxX = Math.max(maxX, c + w / 2);
    } else if (s.from === s.to) {
      maxX = Math.max(maxX, cx[idx[s.from]] + SELF_W + 8 + labelW(lines));
    } else {
      const c = (cx[idx[s.from]] + cx[idx[s.to]]) / 2;
      const half = labelW(lines) / 2;
      minX = Math.min(minX, c - half);
      maxX = Math.max(maxX, c + half);
    }
  });
  const offsetX = MARGIN_X - minX;
  for (let i = 0; i < cx.length; i++) cx[i] += offsetX;

  const headerBottom = HEADER_TOP + BOX_H;
  // Rows are variable height: a wrapped label grows its own row, not the canvas.
  const stepY: number[] = [];
  let cursorY = headerBottom + STEP_TOP_GAP;
  steps.forEach((_, i) => {
    const rowH = STEP_H + (stepLines[i].length - 1) * LINE_H;
    stepY[i] = cursorY + rowH / 2;
    cursorY += rowH;
  });
  const height = cursorY + BOTTOM;
  const lifeBottom = height - BOTTOM;

  const actors: Record<string, Actor> = {};
  participants.forEach((p, i) => {
    const created = p.createAtStep != null && p.createAtStep >= 0 && p.createAtStep < steps.length;
    const rowY = created ? stepY[p.createAtStep as number] : 0;
    actors[p.id] = {
      cx: cx[i],
      boxW: boxW[i],
      boxH: BOX_H,
      boxY: created ? rowY - BOX_H / 2 : HEADER_TOP,
      lifeTop: created ? rowY + BOX_H / 2 : headerBottom,
      createAtStep: created ? (p.createAtStep as number) : undefined,
    };
  });

  const width = maxX + offsetX + MARGIN_X;
  return { width, height, lifeBottom, stepY, stepLines, actors };
}
