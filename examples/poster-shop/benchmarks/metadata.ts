import type { BenchmarkMetadata } from "../../../dev/benchmarks/metadata/types.ts";

const source = "examples/poster-shop/benchmarks/benches/walltime.rs";
const storage = "In-memory Jazz database, local durability";
const canvas = (shapes: number) =>
  `One canvas with ${shapes.toLocaleString("en-US")} shapes across 4 layers, 8 editor cursors, 4 asset-metadata rows and 3 checkpoints.`;

export const posterShopBenchmarks: BenchmarkMetadata[] = [
  ...[512, 4096].map((shapes) => ({
    name: `poster_shop_open_canvas[${shapes}]`,
    title: `PosterShop · open a ${shapes.toLocaleString("en-US")}-shape poster`,
    description:
      "Open every surface the canvas page subscribes to (z-ordered shapes, layers, cursors, asset shelf, checkpoint shelf) and receive each first result.",
    fixture: canvas(shapes),
    storage,
    includes: [
      "Opening five live subscriptions",
      `Materializing all ${(shapes + 19).toLocaleString("en-US")} rows in order`,
      "Dropping the subscriptions",
    ],
    excludes: ["Schema compilation, database opening and seeding", "Rendering"],
    work: {
      count: 1,
      unit: "canvases opened/s",
      explanation: "One complete canvas open per iteration.",
    },
    source,
  })),
  {
    name: "poster_shop_add_shape[4096]",
    title: "PosterShop · draw a shape on a live canvas",
    description:
      "Insert one shape on top of a canvas whose ordered shape and cursor subscriptions are live, and wait until the canvas subscription delivers it. The cost currently grows with the number of shapes on the canvas, not with the one added row (#2086): about 4 ms at 512 shapes and 100 ms at 4,096 locally.",
    fixture: `${canvas(4096)} Each iteration adds one more shape.`,
    storage,
    includes: [
      "Shape insert until local durability",
      "Incremental update of the live ordered canvas until its delta arrives",
    ],
    excludes: ["Opening the live subscriptions", "Sync to other editors", "Rendering"],
    work: { count: 1, unit: "shapes drawn/s", explanation: "One inserted shape per iteration." },
    source,
  },
  {
    name: "poster_shop_move_cursor[4096]",
    title: "PosterShop · a collaborator's cursor moves",
    description:
      "Update one editor's cursor position while the 4,096-shape canvas and the cursor surface are subscribed, and wait until the cursor subscription delivers it. The canvas subscription is not woken.",
    fixture: canvas(4096),
    storage,
    includes: [
      "Cursor update until local durability",
      "Incremental update of the live cursor subscription until its delta arrives",
    ],
    excludes: ["Opening the live subscriptions", "Presence transport", "Rendering"],
    work: { count: 1, unit: "cursor moves/s", explanation: "One cursor update per iteration." },
    source,
  },
];
