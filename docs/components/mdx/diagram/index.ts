// Public API of the diagram engine — the single entrypoint for consumers
// (the would-be package surface). Docs imports ONLY from here, never deep into
// the island; extraction later ≈ move this folder + add a package.json.

export { Graph } from "./graph";
export type {
  GraphProps,
  GraphNode,
  GraphEdge,
  GraphSlot,
  GraphOverlay,
  GraphOverlayCtx,
  GraphGeometryListener,
} from "./graph";

export { bfsWaves, playWaves, hopEdgeId } from "./presets";
export type { Hop } from "./presets";

export { Sequence } from "./sequence";
export type { SequenceProps, Participant, SequenceStep } from "./sequence";

export { NodeShell, NodeIcon, NodeTitle, NodeSubtitle, NodeFooter, NodeAction } from "./kit";

export { DiagramFrame } from "./frame";
export { DiagramStyles } from "./styles";
export { PhoneChrome } from "./phone-chrome";

export { useDiagramTraces, traceDuration } from "./traces";
export type { DiagramTraces, TraceSpec } from "./traces";

export {
  connectChain,
  loopRoundedBox,
  roundedPath,
  PATH_INSET,
  DEFAULT_CORNER_RADIUS,
} from "./geometry";
export type { Point, RouteDirection, RoutedEdge, Anchors, LoopSide } from "./geometry";
