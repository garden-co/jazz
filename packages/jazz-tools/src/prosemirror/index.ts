export {
  commonMarkSpec,
  type MappedMarkdown,
  type MarkdownSpec,
  MarkdownWriter,
  type MarkWriter,
  type NodeWriter,
  type Side,
  toMarkdown,
} from "./markdown.js";
export {
  applyMarkdown,
  docPosition,
  isRemoteChange,
  markdownOf,
  markdownSync,
  type MarkdownSyncOptions,
  textPosition,
} from "./sync.js";
export { mapOffset } from "./text-map.js";
export { captureAnchor, captureRange, resolveAnchor, resolveRange } from "./anchors.js";
export {
  DEFAULT_DRIFT_CONFIDENCE,
  DEFAULT_MIN_CONFIDENCE,
  type Overlay,
  type OverlayOptions,
  type PlacedOverlay,
  overlays,
  placedOverlays,
  setOverlays,
} from "./overlays.js";
export {
  type Collaborator,
  capturePresence,
  presenceOverlays,
  samePresence,
  type SelectionPresence,
} from "./presence.js";
