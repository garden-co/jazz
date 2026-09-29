import {
  takeSnapshot,
  type PosterSnapshot,
  type SnapshotLayer,
  type SnapshotShape,
} from "./poster";

export const POSTER_WIDTH = 1080;
export const POSTER_HEIGHT = 1350;

type LayerSeed = Omit<SnapshotLayer, "id"> & { key: string };
type ShapeSeed = Omit<SnapshotShape, "id" | "layerId" | "assetId"> & { layer: string };

const LAYERS: LayerSeed[] = [
  { key: "background", name: "Background", zIndex: 0, visible: true, locked: true },
  { key: "artwork", name: "Artwork", zIndex: 1, visible: true, locked: false },
  { key: "type", name: "Type", zIndex: 2, visible: true, locked: false },
];

const box = (x: number, y: number, width: number, height: number, rotation = 0) => ({
  x,
  y,
  width,
  height,
  rotation,
});

const SHAPES: ShapeSeed[] = [
  { layer: "background", kind: "rect", ...box(0, 0, 1080, 1350), zIndex: 0, fill: "cream" },
  { layer: "artwork", kind: "ellipse", ...box(190, 120, 700, 700), zIndex: 0, fill: "tangerine" },
  { layer: "artwork", kind: "ellipse", ...box(340, 270, 400, 400), zIndex: 1, fill: "red" },
  { layer: "artwork", kind: "rect", ...box(-80, 700, 1240, 96, -8), zIndex: 2, fill: "blue" },
  { layer: "artwork", kind: "rect", ...box(-80, 830, 1240, 36, -8), zIndex: 3, fill: "ink" },
  {
    layer: "type",
    kind: "text",
    ...box(80, 960, 920, 170),
    zIndex: 0,
    text: "Late set",
    fill: "ink",
  },
  {
    layer: "type",
    kind: "text",
    ...box(84, 1160, 760, 56),
    zIndex: 1,
    text: "Friday 14 November · doors 8pm",
    fill: "ink",
  },
  {
    layer: "type",
    kind: "text",
    ...box(84, 1236, 600, 56),
    zIndex: 2,
    text: "The Garden Room",
    fill: "red",
  },
];

export type DemoPoster = {
  title: string;
  layers: SnapshotLayer[];
  shapes: SnapshotShape[];
  checkpoint: { label: string; snapshot: PosterSnapshot };
};

/**
 * The demo poster every new account starts with. Content, order and geometry
 * are fixed; only row ids come from `newId`, so the seed is deterministic for
 * a deterministic id source.
 */
export function demoPoster(displayName: string, newId: () => string): DemoPoster {
  const layerIds = new Map(LAYERS.map((layer) => [layer.key, newId()]));
  const layers = LAYERS.map(({ key, ...layer }) => ({ ...layer, id: layerIds.get(key)! }));
  const shapes = SHAPES.map(({ layer, ...shape }) => ({
    ...shape,
    id: newId(),
    layerId: layerIds.get(layer)!,
    assetId: null,
    text: shape.text ?? null,
  }));
  return {
    title: `${displayName.trim() || "Untitled"}'s poster`,
    layers,
    shapes,
    checkpoint: { label: "First draft", snapshot: takeSnapshot(layers, shapes) },
  };
}
