// Lens-diagram geometry — a single CCW path that snakes through the data card,
// every schema/lens card on the projection chain, and out to the client. Pure:
// it consumes engine-measured Anchors (natural coords), no DOM, so it inherits
// the engine's offset-based measurement + scale-to-fit for free.

import { type Anchors, DEFAULT_CORNER_RADIUS, loopRoundedBox, PATH_INSET } from "./diagram";

export type Version = 1 | 2 | 3;
export type Direction = "forward" | "backward" | "na";

export function getDirection(from: Version, to: Version): Direction {
  if (from === to) return "na";
  return from < to ? "forward" : "backward";
}

export const schemaId = (v: Version) => `schema-${v}`;
export const lensId = (idx: number) => `lens-${idx}`;
export const dataId = (v: Version) => `data-${v}`;
export const CLIENT_ID = "client";

export type Card = { type: "schema" | "lens"; id: string };

// Ordered schema/lens cards on the projection chain between data and client.
export function collectCards(dataVersion: Version, client: Version): Card[] {
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
    cards.push({ type: "schema", id: schemaId(v) });
    if (i < versions.length - 1) {
      const idx = Math.min(v, versions[i + 1]) - 1;
      cards.push({ type: "lens", id: lensId(idx) });
    }
  }
  return cards;
}

type LoopStart = "left" | "top" | "bottom";

function loopSchema(a: Anchors, start: LoopStart): string {
  const entry = start === "left" ? a.midY : a.midX;
  return loopRoundedBox(a, entry, start);
}

// Lens cards use a pill silhouette (borderRadius "50% / 35%"): straight sides,
// elliptical caps. Derived from the inset Anchors.
function loopLens(a: Anchors, start: LoopStart): string {
  const { left, right, top, bottom, midX, midY } = a;
  const innerW = right - left;
  const innerH = bottom - top;
  const rx = innerW / 2;
  const ry = (innerH + 2 * PATH_INSET) * 0.35 - PATH_INSET;
  switch (start) {
    case "left":
      return `L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom} A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top} A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${midY}`;
    case "top":
      return `A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom} A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top}`;
    case "bottom":
      return `A ${rx} ${ry} 0 0 0 ${right} ${bottom - ry} L ${right} ${top + ry} A ${rx} ${ry} 0 0 0 ${midX} ${top} A ${rx} ${ry} 0 0 0 ${left} ${top + ry} L ${left} ${bottom - ry} A ${rx} ${ry} 0 0 0 ${midX} ${bottom}`;
  }
}

export function buildPath(args: {
  cards: Card[];
  anchors: Record<string, Anchors>;
  dataId: string;
  clientId: string;
  direction: Direction;
}): string {
  const { cards, anchors, dataId: dId, clientId, direction } = args;
  const data = anchors[dId];
  const client = anchors[clientId];
  const first = cards[0] && anchors[cards[0].id];
  if (!data || !client || !first) return "";
  const back = direction === "backward";
  const busX = first.midX;
  const firstMidY = first.midY;

  let path = `M ${data.right} ${firstMidY}`;
  path += ` L ${first.left} ${firstMidY}`;

  for (let i = 0; i < cards.length; i++) {
    const a = anchors[cards[i].id];
    if (!a) return "";
    const isLens = cards[i].type === "lens";
    const isFirst = i === 0;
    const isLast = i === cards.length - 1;
    const { right, midX, midY, top, bottom } = a;
    const start: LoopStart = isFirst ? "left" : back ? "bottom" : "top";
    path += " " + (isLens ? loopLens(a, start) : loopSchema(a, start));

    if (isFirst && isLast) {
      path += ` L ${right} ${midY} L ${client.left} ${midY}`;
    } else if (isFirst) {
      path += ` L ${midX} ${midY} L ${midX} ${back ? top : bottom}`;
    } else if (isLast) {
      path += ` L ${midX} ${midY} L ${right} ${midY} L ${client.left} ${midY}`;
    } else {
      path += ` L ${midX} ${back ? top : bottom}`;
    }

    if (!isLast) {
      const next = anchors[cards[i + 1].id];
      if (!next) return "";
      path += ` L ${busX} ${back ? next.bottom : next.top}`;
    }
  }
  return path;
}

export { DEFAULT_CORNER_RADIUS };
