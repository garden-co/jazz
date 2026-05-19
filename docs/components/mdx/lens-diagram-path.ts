// Lens-diagram geometry — builds a single CCW path that snakes through the
// data card, every schema/lens card on the projection chain, and out to the
// client. Shared geometry primitives live in ./diagram/geometry.

import {
  PATH_INSET,
  DEFAULT_CORNER_RADIUS,
  type Anchors,
  insetRect,
  loopRoundedBox,
} from "./diagram/geometry";

export type Version = 1 | 2 | 3;
export type Direction = "forward" | "backward" | "na";

export function getDirection(from: Version, to: Version): Direction {
  if (from === to) return "na";
  return from < to ? "forward" : "backward";
}

function unreachable(x: never): never {
  throw new Error(`unreachable: ${String(x)}`);
}

type LoopStart = "left" | "top" | "bottom";

// Schema cards are rounded-lg → DEFAULT_CORNER_RADIUS works directly.
function loopSchema(rect: DOMRect, container: DOMRect, start: LoopStart): string {
  const a = insetRect(rect, container);
  const entry = start === "left" ? a.midY : a.midX;
  return loopRoundedBox(a, entry, start);
}

// Lens cards use a pill/ellipse shape (borderRadius "50% / 35%"). The path
// hugs that silhouette — straight sides, elliptical top and bottom caps.
function loopLens(rect: DOMRect, container: DOMRect, start: LoopStart): string {
  const a = insetRect(rect, container);
  const { left, right, top, bottom, midX, midY } = a;
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
    const a: Anchors = insetRect(rect, c);
    const { right, midX, midY, top, bottom } = a;

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

// Re-export the unused symbol so it lives in the bundle if anything elsewhere
// references it. (Kept to mirror the previous public surface.)
export { DEFAULT_CORNER_RADIUS };
