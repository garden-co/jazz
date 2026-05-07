// Pure geometry / path helpers for the lens diagram. No React, no DOM mutation
// — just functions over DOMRects that produce SVG path strings.

export type Version = 1 | 2 | 3;
export type Direction = "forward" | "backward" | "na";

export function getDirection(from: Version, to: Version): Direction {
  if (from === to) return "na";
  return from < to ? "forward" : "backward";
}

function unreachable(x: never): never {
  throw new Error(`unreachable: ${String(x)}`);
}

const PATH_INSET = 1; // 2px CSS border, 2px SVG stroke → inset stroke centreline by 1px
const SCHEMA_RADIUS = 8 - PATH_INSET; // rounded-lg = 8px outer; centreline radius

type InsetRect = {
  left: number;
  right: number;
  top: number;
  bottom: number;
  midX: number;
  midY: number;
};

function insetRect(rect: DOMRect, container: DOMRect): InsetRect {
  return {
    left: rect.left - container.left + PATH_INSET,
    right: rect.right - container.left - PATH_INSET,
    top: rect.top - container.top + PATH_INSET,
    bottom: rect.bottom - container.top - PATH_INSET,
    midX: (rect.left + rect.right) / 2 - container.left,
    midY: (rect.top + rect.bottom) / 2 - container.top,
  };
}

type LoopStart = "left" | "top" | "bottom";

// CCW perimeter loop around a schema, starting and ending at the same midpoint.
function loopSchema(rect: DOMRect, container: DOMRect, start: LoopStart): string {
  const { left, right, top, bottom, midX, midY } = insetRect(rect, container);
  const r = SCHEMA_RADIUS;
  switch (start) {
    case "left":
      return `L ${left} ${bottom - r} A ${r} ${r} 0 0 0 ${left + r} ${bottom} L ${right - r} ${bottom} A ${r} ${r} 0 0 0 ${right} ${bottom - r} L ${right} ${top + r} A ${r} ${r} 0 0 0 ${right - r} ${top} L ${left + r} ${top} A ${r} ${r} 0 0 0 ${left} ${top + r} L ${left} ${midY}`;
    case "top":
      return `L ${left + r} ${top} A ${r} ${r} 0 0 0 ${left} ${top + r} L ${left} ${bottom - r} A ${r} ${r} 0 0 0 ${left + r} ${bottom} L ${right - r} ${bottom} A ${r} ${r} 0 0 0 ${right} ${bottom - r} L ${right} ${top + r} A ${r} ${r} 0 0 0 ${right - r} ${top} L ${midX} ${top}`;
    case "bottom":
      return `L ${right - r} ${bottom} A ${r} ${r} 0 0 0 ${right} ${bottom - r} L ${right} ${top + r} A ${r} ${r} 0 0 0 ${right - r} ${top} L ${left + r} ${top} A ${r} ${r} 0 0 0 ${left} ${top + r} L ${left} ${bottom - r} A ${r} ${r} 0 0 0 ${left + r} ${bottom} L ${midX} ${bottom}`;
    default:
      return unreachable(start);
  }
}

// CCW perimeter loop around a lens.
// borderRadius "50% / 35%" → outer rx = w/2, ry = h*0.35; centreline shifts in by 1px.
function loopLens(rect: DOMRect, container: DOMRect, start: LoopStart): string {
  const { left, right, top, bottom, midX, midY } = insetRect(rect, container);
  const rx = rect.width / 2 - PATH_INSET;
  const ry = rect.height * 0.35 - PATH_INSET;
  switch (start) {
    case "left":
      return `L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom} A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top} A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${midY}`;
    case "top":
      return `A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom} A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top}`;
    case "bottom":
      return `A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top} A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom}`;
    default:
      return unreachable(start);
  }
}

export type Card = { type: "schema" | "lens"; el: HTMLElement };

export function collectCards(
  dataVersion: Version,
  client: Version,
  schemaRefs: Partial<Record<Version, HTMLDivElement | null>>,
  lensRefs: Partial<Record<number, HTMLDivElement | null>>,
): Card[] | null {
  const back = dataVersion > client;
  const lo = Math.min(dataVersion, client);
  const hi = Math.max(dataVersion, client);
  const versions: Version[] = [];
  if (back) {
    for (let v = hi; v >= lo; v--) versions.push(v as Version);
  } else {
    for (let v = lo; v <= hi; v++) versions.push(v as Version);
  }

  const cards: Card[] = [];
  for (let i = 0; i < versions.length; i++) {
    const v = versions[i];
    const sEl = schemaRefs[v];
    if (!sEl) return null;
    cards.push({ type: "schema", el: sEl });
    if (i < versions.length - 1) {
      // Lens between schemas v and v+1 lives at index min(v, v+1) - 1.
      const lensIdx = Math.min(v, versions[i + 1]) - 1;
      const lEl = lensRefs[lensIdx];
      if (!lEl) return null;
      cards.push({ type: "lens", el: lEl });
    }
  }
  return cards;
}

export function buildPath(args: {
  cards: Card[];
  rects: DOMRect[];
  container: DOMRect;
  data: DOMRect;
  client: DOMRect;
  direction: Direction;
}): string {
  const { cards, rects, container: c, data: d, client: cl, direction } = args;
  const back = direction === "backward";
  const firstRect = rects[0];
  const busX = (firstRect.left + firstRect.right) / 2 - c.left;
  const firstMidY = (firstRect.top + firstRect.bottom) / 2 - c.top;

  let path = `M ${d.right - c.left} ${firstMidY}`;
  path += ` L ${firstRect.left - c.left + PATH_INSET} ${firstMidY}`;

  for (let i = 0; i < cards.length; i++) {
    const rect = rects[i];
    const isLens = cards[i].type === "lens";
    const isFirst = i === 0;
    const isLast = i === cards.length - 1;
    const { right, top, bottom, midX, midY } = insetRect(rect, c);

    // First card enters at left-mid (from the data card); subsequent cards
    // enter at top-mid (forward/idle) or bottom-mid (backward).
    const start: LoopStart = isFirst ? "left" : back ? "bottom" : "top";
    path += " " + (isLens ? loopLens(rect, c, start) : loopSchema(rect, c, start));

    // Body cross from where the loop ended toward the next destination.
    if (isFirst && isLast) {
      path += ` L ${right} ${midY} L ${cl.left - c.left} ${midY}`;
    } else if (isFirst) {
      path += ` L ${midX} ${midY} L ${midX} ${back ? top : bottom}`;
    } else if (isLast) {
      path += ` L ${midX} ${midY} L ${right} ${midY} L ${cl.left - c.left} ${midY}`;
    } else {
      path += ` L ${midX} ${back ? top : bottom}`;
    }

    if (!isLast) {
      const nextRect = rects[i + 1];
      const nextEntryY = back
        ? nextRect.bottom - c.top - PATH_INSET
        : nextRect.top - c.top + PATH_INSET;
      path += ` L ${busX} ${nextEntryY}`;
    }
  }

  return path;
}
