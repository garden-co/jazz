/**
 * Pure poster model helpers shared by the canvas, the bootstrap seed and the
 * checkpoint preview. Nothing here touches Jazz, so it is unit-testable.
 */

/** Shape fills are stored as palette keys, never raw colours. Each key maps to
 * a design-system data token, so the poster stays on the shared palette. */
export const PALETTE = [
  { key: "ink", label: "Ink", token: "--color-data-gray-5" },
  { key: "paper", label: "Paper", token: "--color-data-gray-1" },
  { key: "cream", label: "Cream", token: "--color-data-yellow-1" },
  { key: "sun", label: "Sun", token: "--color-data-yellow-3" },
  { key: "tangerine", label: "Tangerine", token: "--color-data-orange-3" },
  { key: "red", label: "Red", token: "--color-data-red-4" },
  { key: "pink", label: "Pink", token: "--color-data-pink-3" },
  { key: "violet", label: "Violet", token: "--color-data-purple-4" },
  { key: "blue", label: "Blue", token: "--color-data-blue-4" },
  { key: "teal", label: "Teal", token: "--color-data-teal-3" },
] as const;

export type PaletteKey = (typeof PALETTE)[number]["key"];

/** Resolve a stored fill to a CSS colour. Unknown values (for example rows
 * written by older clients) fall back to ink instead of reaching the DOM. */
export function fillColor(fill: string): string {
  const entry = PALETTE.find((colour) => colour.key === fill) ?? PALETTE[0];
  return `var(${entry.token})`;
}

/** Collaborator colours come from the categorical data tokens. */
const CURSOR_TOKENS = [
  "--color-data-categorical-blue",
  "--color-data-categorical-pink",
  "--color-data-categorical-green",
  "--color-data-categorical-orange",
  "--color-data-categorical-purple",
  "--color-data-categorical-teal",
  "--color-data-categorical-red",
  "--color-data-categorical-indigo",
] as const;

export function cursorColorKey(author: string): string {
  let hash = 0;
  for (const char of author) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return String(hash % CURSOR_TOKENS.length);
}

export function cursorColor(key: string): string {
  const index = Number.parseInt(key, 10);
  return `var(${CURSOR_TOKENS[Number.isInteger(index) ? Math.abs(index) % CURSOR_TOKENS.length : 0]})`;
}

export type ShapeKind = "rect" | "ellipse" | "text" | "image";

export type ShapeGeometry = {
  x: number;
  y: number;
  width: number;
  height: number;
  rotation: number;
};

export type SnapshotLayer = {
  id: string;
  name: string;
  zIndex: number;
  visible: boolean;
  locked: boolean;
};

export type SnapshotShape = ShapeGeometry & {
  id: string;
  layerId: string;
  assetId: string | null;
  kind: ShapeKind;
  zIndex: number;
  text: string | null;
  fill: string;
};

export type PosterSnapshot = {
  version: 1;
  layers: SnapshotLayer[];
  shapes: SnapshotShape[];
};

export const MIN_SHAPE_SIZE = 16;

/** Keep only the durable fields a checkpoint needs. */
export function takeSnapshot(
  layers: readonly SnapshotLayer[],
  shapes: readonly (Omit<SnapshotShape, "assetId" | "text"> & {
    assetId?: string | null;
    text?: string | null;
  })[],
): PosterSnapshot {
  return {
    version: 1,
    layers: layers.map(({ id, name, zIndex, visible, locked }) => ({
      id,
      name,
      zIndex,
      visible,
      locked,
    })),
    shapes: shapes.map((shape) => ({
      id: shape.id,
      layerId: shape.layerId,
      assetId: shape.assetId ?? null,
      kind: shape.kind,
      x: shape.x,
      y: shape.y,
      width: shape.width,
      height: shape.height,
      rotation: shape.rotation,
      zIndex: shape.zIndex,
      text: shape.text ?? null,
      fill: shape.fill,
    })),
  };
}

const SHAPE_KINDS = new Set<ShapeKind>(["rect", "ellipse", "text", "image"]);

/** Checkpoint snapshots arrive as untyped JSON; validate before rendering. */
export function parseSnapshot(value: unknown): PosterSnapshot | null {
  if (!isRecord(value) || value.version !== 1) return null;
  if (!Array.isArray(value.layers) || !Array.isArray(value.shapes)) return null;
  const layers: SnapshotLayer[] = [];
  for (const layer of value.layers) {
    if (
      !isRecord(layer) ||
      typeof layer.id !== "string" ||
      typeof layer.name !== "string" ||
      typeof layer.zIndex !== "number" ||
      typeof layer.visible !== "boolean"
    )
      return null;
    layers.push({
      id: layer.id,
      name: layer.name,
      zIndex: layer.zIndex,
      visible: layer.visible,
      locked: layer.locked === true,
    });
  }
  const shapes: SnapshotShape[] = [];
  for (const shape of value.shapes) {
    if (
      !isRecord(shape) ||
      typeof shape.id !== "string" ||
      typeof shape.layerId !== "string" ||
      typeof shape.kind !== "string" ||
      !SHAPE_KINDS.has(shape.kind as ShapeKind) ||
      typeof shape.fill !== "string"
    )
      return null;
    const numbers = ["x", "y", "width", "height", "rotation", "zIndex"] as const;
    if (numbers.some((key) => typeof shape[key] !== "number")) return null;
    shapes.push({
      id: shape.id,
      layerId: shape.layerId,
      assetId: typeof shape.assetId === "string" ? shape.assetId : null,
      kind: shape.kind as ShapeKind,
      x: shape.x as number,
      y: shape.y as number,
      width: shape.width as number,
      height: shape.height as number,
      rotation: shape.rotation as number,
      zIndex: shape.zIndex as number,
      text: typeof shape.text === "string" ? shape.text : null,
      fill: shape.fill,
    });
  }
  return { version: 1, layers, shapes };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Paint order: layers by zIndex, then shapes by zIndex inside their layer.
 * Equal z indexes keep a stable id order rather than an invented merge rule. */
export function paintOrder<
  L extends { id: string; zIndex: number },
  S extends { id: string; layerId: string; zIndex: number },
>(layers: readonly L[], shapes: readonly S[]): { layer: L; shapes: S[] }[] {
  const byLayer = new Map<string, S[]>();
  for (const shape of shapes) {
    const list = byLayer.get(shape.layerId) ?? [];
    list.push(shape);
    byLayer.set(shape.layerId, list);
  }
  return [...layers].sort(compareZ).map((layer) => ({
    layer,
    shapes: (byLayer.get(layer.id) ?? []).sort(compareZ),
  }));
}

function compareZ(a: { id: string; zIndex: number }, b: { id: string; zIndex: number }) {
  return a.zIndex - b.zIndex || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
}

/**
 * Move one item a step within an ordered list and return the z indexes that
 * need writing. The list is renumbered 0..n-1 so duplicate indexes left by
 * concurrent inserts are resolved deterministically by the next reorder.
 */
export function reorder<T extends { id: string; zIndex: number }>(
  items: readonly T[],
  id: string,
  direction: "up" | "down",
): { id: string; zIndex: number }[] {
  const ordered = [...items].sort(compareZ);
  const index = ordered.findIndex((item) => item.id === id);
  const target = direction === "up" ? index + 1 : index - 1;
  if (index < 0 || target < 0 || target >= ordered.length) return [];
  [ordered[index], ordered[target]] = [ordered[target]!, ordered[index]!];
  return ordered
    .map((item, zIndex) => ({ id: item.id, zIndex, previous: item.zIndex }))
    .filter((item) => item.zIndex !== item.previous)
    .map(({ id, zIndex }) => ({ id, zIndex }));
}

export function nextZIndex(items: readonly { zIndex: number }[]): number {
  return items.reduce((max, item) => Math.max(max, item.zIndex + 1), 0);
}

/**
 * Resize from a corner handle. `dx`/`dy` are pointer deltas in poster space;
 * they are rotated into the shape's own frame so rotated shapes resize along
 * their own edges, and the opposite corner stays put.
 */
export function resizeFromCorner(
  start: ShapeGeometry,
  corner: "nw" | "ne" | "sw" | "se",
  dx: number,
  dy: number,
): ShapeGeometry {
  const angle = (-start.rotation * Math.PI) / 180;
  const localDx = dx * Math.cos(angle) - dy * Math.sin(angle);
  const localDy = dx * Math.sin(angle) + dy * Math.cos(angle);
  const west = corner === "nw" || corner === "sw";
  const north = corner === "nw" || corner === "ne";
  const width = Math.max(MIN_SHAPE_SIZE, start.width + (west ? -localDx : localDx));
  const height = Math.max(MIN_SHAPE_SIZE, start.height + (north ? -localDy : localDy));
  // Keep the opposite corner fixed in poster space.
  const theta = (start.rotation * Math.PI) / 180;
  const ox = west ? start.width : 0;
  const oy = north ? start.height : 0;
  const nx = west ? width : 0;
  const ny = north ? height : 0;
  const anchorBefore = rotateAround(start, start.width, start.height, ox, oy, theta);
  const draft = { ...start, width, height };
  const anchorAfter = rotateAround(draft, width, height, nx, ny, theta);
  return {
    ...draft,
    x: round(start.x + anchorBefore.x - anchorAfter.x),
    y: round(start.y + anchorBefore.y - anchorAfter.y),
    width: round(width),
    height: round(height),
  };
}

/** Poster-space position of local point (px, py) of a rotated box. */
function rotateAround(
  box: { x: number; y: number },
  width: number,
  height: number,
  px: number,
  py: number,
  theta: number,
) {
  const cx = width / 2;
  const cy = height / 2;
  const rx = px - cx;
  const ry = py - cy;
  return {
    x: box.x + cx + rx * Math.cos(theta) - ry * Math.sin(theta),
    y: box.y + cy + rx * Math.sin(theta) + ry * Math.cos(theta),
  };
}

export function round(value: number): number {
  return Math.round(value * 10) / 10;
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
