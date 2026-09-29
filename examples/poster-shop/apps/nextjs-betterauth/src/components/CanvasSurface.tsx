"use client";

import { Banner, Button, HStack, IconButton, Toolbar } from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { ArrowDownToLine, ArrowUpToLine, Circle, Copy, Square, Trash2, Type } from "lucide-react";
import {
  memo,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent,
} from "react";
import { app, type Layer, type Shape } from "@/schema";
import { CursorLayer, useCursorPublisher } from "@/src/components/CollaboratorCursors";
import {
  clamp,
  fillColor,
  nextZIndex,
  paintOrder,
  parseSnapshot,
  reorder,
  resizeFromCorner,
  round,
  type ShapeGeometry,
  type ShapeKind,
  type SnapshotLayer,
  type SnapshotShape,
} from "@/src/lib/poster";
import { useAssetUrlById } from "@/src/lib/use-asset-url";

type Corner = "nw" | "ne" | "sw" | "se";
type Drag =
  | { mode: "move"; id: string; pointerId: number; origin: Point; start: ShapeGeometry }
  | {
      mode: "resize";
      id: string;
      pointerId: number;
      origin: Point;
      start: ShapeGeometry;
      corner: Corner;
    };
type Point = { x: number; y: number };
type RenderShape = SnapshotShape | Shape;

const NEW_SHAPES: Record<
  Exclude<ShapeKind, "image">,
  { width: number; height: number; fill: string; text?: string }
> = {
  rect: { width: 360, height: 240, fill: "blue" },
  ellipse: { width: 320, height: 320, fill: "sun" },
  text: { width: 640, height: 110, fill: "ink", text: "New headline" },
};

/** Handle size in screen pixels; converted to poster units at render time. */
const HANDLE_PX = 12;

export function CanvasSurface({
  canvasId,
  width,
  height,
  canEdit,
  author,
  displayName,
  selectedShapeId,
  onSelectShape,
  activeLayerId,
  previewCheckpointId,
  onExitPreview,
}: {
  canvasId: string;
  width: number;
  height: number;
  canEdit: boolean;
  author: string | null;
  displayName: string;
  selectedShapeId: string | null;
  onSelectShape: (id: string | null) => void;
  activeLayerId: string | null;
  previewCheckpointId: string | null;
  onExitPreview: () => void;
}) {
  const db = useDb();
  // The shape renderer owns exactly these two subscriptions. Cursors live in
  // <CursorLayer>, so presence traffic never re-runs this query.
  const { data: shapes = [] } = useAll(app.shapes.where({ canvasId }).orderBy("zIndex", "asc"));
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "asc"));
  const { data: preview } = useAll(
    previewCheckpointId
      ? app.checkpoints.where({ id: previewCheckpointId }).select("label", "snapshot")
      : undefined,
  );
  const snapshot = preview?.[0] ? parseSnapshot(preview[0].snapshot) : null;

  const svgRef = useRef<SVGSVGElement>(null);
  const unitsPerPixel = useUnitsPerPixel(svgRef, width);
  const cursor = useCursorPublisher(canvasId, author, displayName);
  const [drag, setDrag] = useState<Drag | null>(null);
  const [draft, setDraft] = useState<{ id: string; geometry: ShapeGeometry } | null>(null);
  const writer = useFrameWriter((id: string, geometry: ShapeGeometry) =>
    db.update(app.shapes, id, geometry),
  );

  const layerById = useMemo(() => new Map(layers.map((layer) => [layer.id, layer])), [layers]);
  const isEditable = useCallback(
    (shape: { layerId: string }) => {
      const layer = layerById.get(shape.layerId);
      return canEdit && !!layer && layer.visible && !layer.locked;
    },
    [canEdit, layerById],
  );
  const selected = shapes.find((shape) => shape.id === selectedShapeId) ?? null;
  const selectedEditable = selected && isEditable(selected) ? selected : null;

  // Deselect when the selection disappears (deleted elsewhere, hidden, or
  // locked). A just-inserted shape may not be in the query result yet, so a
  // missing row only counts once it has been seen.
  const seenSelection = useRef<string | null>(null);
  if (selected) seenSelection.current = selected.id;
  useEffect(() => {
    if (!selectedShapeId || drag) return;
    const vanished = !selected && seenSelection.current === selectedShapeId;
    if ((selected && !selectedEditable) || vanished) onSelectShape(null);
  }, [selectedShapeId, selected, selectedEditable, drag, onSelectShape]);

  const toPoster = (event: { clientX: number; clientY: number }): Point => {
    const svg = svgRef.current;
    const matrix = svg?.getScreenCTM();
    if (!svg || !matrix) return { x: 0, y: 0 };
    const point = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse());
    return { x: point.x, y: point.y };
  };

  const startDrag = (event: PointerEvent, shape: Shape, corner?: Corner) => {
    if (event.button !== 0) return;
    event.stopPropagation();
    onSelectShape(shape.id);
    svgRef.current?.setPointerCapture(event.pointerId);
    const start = geometryOf(shape);
    const origin = toPoster(event);
    setDrag(
      corner
        ? { mode: "resize", id: shape.id, pointerId: event.pointerId, origin, start, corner }
        : { mode: "move", id: shape.id, pointerId: event.pointerId, origin, start },
    );
    setDraft({ id: shape.id, geometry: start });
  };

  const onPointerMove = (event: PointerEvent<SVGSVGElement>) => {
    const point = toPoster(event);
    if (
      !previewCheckpointId &&
      point.x >= 0 &&
      point.y >= 0 &&
      point.x <= width &&
      point.y <= height
    )
      cursor.move(round(point.x), round(point.y));
    else cursor.leave();
    if (!drag || event.pointerId !== drag.pointerId) return;
    const dx = point.x - drag.origin.x;
    const dy = point.y - drag.origin.y;
    const geometry =
      drag.mode === "move"
        ? {
            ...drag.start,
            x: round(clamp(drag.start.x + dx, -drag.start.width + 8, width - 8)),
            y: round(clamp(drag.start.y + dy, -drag.start.height + 8, height - 8)),
          }
        : resizeFromCorner(drag.start, drag.corner, dx, dy);
    setDraft({ id: drag.id, geometry });
    writer.schedule(drag.id, geometry);
  };

  const endDrag = (event: PointerEvent<SVGSVGElement>) => {
    if (!drag || event.pointerId !== drag.pointerId) return;
    writer.flush();
    setDrag(null);
    setDraft(null);
  };

  const addShape = (kind: Exclude<ShapeKind, "image">) => {
    const layer = targetLayer(layers, activeLayerId);
    if (!layer) return;
    const template = NEW_SHAPES[kind];
    const inLayer = shapes.filter((shape) => shape.layerId === layer.id);
    const offset = (inLayer.length % 6) * 24;
    const { value } = db.insert(app.shapes, {
      canvasId,
      layerId: layer.id,
      kind,
      x: round((width - template.width) / 2 + offset),
      y: round((height - template.height) / 2 + offset),
      width: template.width,
      height: template.height,
      rotation: 0,
      zIndex: nextZIndex(inLayer),
      fill: template.fill,
      ...(template.text ? { text: template.text } : {}),
    });
    onSelectShape(value.id);
  };

  const duplicate = (shape: Shape) => {
    const inLayer = shapes.filter((other) => other.layerId === shape.layerId);
    const { value } = db.insert(app.shapes, {
      canvasId,
      layerId: shape.layerId,
      kind: shape.kind,
      x: shape.x + 32,
      y: shape.y + 32,
      width: shape.width,
      height: shape.height,
      rotation: shape.rotation,
      zIndex: nextZIndex(inLayer),
      fill: shape.fill,
      ...(shape.assetId ? { assetId: shape.assetId } : {}),
      ...(shape.text != null ? { text: shape.text } : {}),
    });
    onSelectShape(value.id);
  };

  const arrange = async (shape: Shape, direction: "up" | "down") => {
    const moves = reorder(
      shapes.filter((other) => other.layerId === shape.layerId),
      shape.id,
      direction,
    );
    if (moves.length === 0) return;
    await db.transaction((tx) => {
      for (const move of moves) tx.update(app.shapes, move.id, { zIndex: move.zIndex });
    });
  };

  const remove = (shape: Shape) => {
    db.delete(app.shapes, shape.id);
    onSelectShape(null);
  };

  const onKeyDown = (event: KeyboardEvent<SVGSVGElement>) => {
    if (!selectedEditable) return;
    if (event.key === "Delete" || event.key === "Backspace") {
      event.preventDefault();
      remove(selectedEditable);
      return;
    }
    const step = event.shiftKey ? 10 : 1;
    const nudge: Record<string, Point> = {
      ArrowLeft: { x: -step, y: 0 },
      ArrowRight: { x: step, y: 0 },
      ArrowUp: { x: 0, y: -step },
      ArrowDown: { x: 0, y: step },
    };
    const delta = nudge[event.key];
    if (!delta) return;
    event.preventDefault();
    db.update(app.shapes, selectedEditable.id, {
      x: selectedEditable.x + delta.x,
      y: selectedEditable.y + delta.y,
    });
  };

  const hasTarget = !!targetLayer(layers, activeLayerId);
  const liveOrder = paintOrder(layers, shapes);
  const order: { layer: SnapshotLayer | Layer; shapes: RenderShape[] }[] = snapshot
    ? paintOrder(snapshot.layers, snapshot.shapes)
    : liveOrder;

  return (
    <div className="canvas-column">
      {previewCheckpointId ? (
        <Banner
          status="info"
          title={preview?.[0] ? `Previewing “${preview[0].label}”` : "Loading checkpoint"}
          description="This is the poster as it was saved. Editing is paused until you return to the live poster."
          endContent={<Button label="Back to live" variant="secondary" onClick={onExitPreview} />}
        />
      ) : (
        <Toolbar
          label="Poster tools"
          startContent={
            <HStack gap={1}>
              <IconButton
                label="Add rectangle"
                tooltip="Add rectangle"
                variant="ghost"
                icon={<Square />}
                isDisabled={!canEdit || !hasTarget}
                onClick={() => addShape("rect")}
              />
              <IconButton
                label="Add ellipse"
                tooltip="Add ellipse"
                variant="ghost"
                icon={<Circle />}
                isDisabled={!canEdit || !hasTarget}
                onClick={() => addShape("ellipse")}
              />
              <IconButton
                label="Add text"
                tooltip="Add text"
                variant="ghost"
                icon={<Type />}
                isDisabled={!canEdit || !hasTarget}
                onClick={() => addShape("text")}
              />
            </HStack>
          }
          endContent={
            <HStack gap={1}>
              <IconButton
                label="Bring forward"
                tooltip="Bring forward"
                variant="ghost"
                icon={<ArrowUpToLine />}
                isDisabled={!selectedEditable}
                onClick={() => selectedEditable && void arrange(selectedEditable, "up")}
              />
              <IconButton
                label="Send backward"
                tooltip="Send backward"
                variant="ghost"
                icon={<ArrowDownToLine />}
                isDisabled={!selectedEditable}
                onClick={() => selectedEditable && void arrange(selectedEditable, "down")}
              />
              <IconButton
                label="Duplicate"
                tooltip="Duplicate"
                variant="ghost"
                icon={<Copy />}
                isDisabled={!selectedEditable}
                onClick={() => selectedEditable && duplicate(selectedEditable)}
              />
              <IconButton
                label="Delete shape"
                tooltip="Delete shape"
                variant="ghost"
                icon={<Trash2 />}
                isDisabled={!selectedEditable}
                onClick={() => selectedEditable && remove(selectedEditable)}
              />
            </HStack>
          }
        />
      )}
      <div className="artboard-frame">
        <svg
          ref={svgRef}
          className="artboard"
          viewBox={`0 0 ${width} ${height}`}
          role="application"
          aria-label="Poster canvas"
          aria-roledescription="poster canvas"
          tabIndex={0}
          data-shape-count={shapes.length}
          data-preview={previewCheckpointId ? "true" : undefined}
          onPointerDown={() => onSelectShape(null)}
          onPointerMove={onPointerMove}
          onPointerUp={endDrag}
          onPointerCancel={endDrag}
          onPointerLeave={cursor.leave}
          onKeyDown={onKeyDown}
        >
          <defs>
            <clipPath id={`artboard-${canvasId}`}>
              <rect width={width} height={height} />
            </clipPath>
          </defs>
          <rect width={width} height={height} fill={fillColor("paper")} />
          <g clipPath={`url(#artboard-${canvasId})`}>
            {order.map(({ layer, shapes: layerShapes }) =>
              layer.visible ? (
                <g key={layer.id} data-layer={layer.name}>
                  {layerShapes.map((shape) => {
                    const geometry =
                      draft && draft.id === shape.id ? draft.geometry : geometryOf(shape);
                    const editable = !snapshot && isEditable(shape);
                    return (
                      <ShapeView
                        key={shape.id}
                        shape={shape}
                        geometry={geometry}
                        onPointerDown={
                          editable ? (event) => startDrag(event, shape as Shape) : undefined
                        }
                      />
                    );
                  })}
                </g>
              ) : null,
            )}
          </g>
          {selectedEditable && !snapshot && (
            <SelectionFrame
              geometry={
                draft && draft.id === selectedEditable.id
                  ? draft.geometry
                  : geometryOf(selectedEditable)
              }
              handleSize={HANDLE_PX * unitsPerPixel}
              onHandleDown={(event, corner) => startDrag(event, selectedEditable, corner)}
            />
          )}
          {!previewCheckpointId && (
            <CursorLayer canvasId={canvasId} author={author} unitsPerPixel={unitsPerPixel} />
          )}
        </svg>
      </div>
    </div>
  );
}

function geometryOf(shape: ShapeGeometry): ShapeGeometry {
  return {
    x: shape.x,
    y: shape.y,
    width: shape.width,
    height: shape.height,
    rotation: shape.rotation,
  };
}

/** New shapes go to the chosen layer, else the top-most editable one. */
function targetLayer(layers: readonly Layer[], activeLayerId: string | null) {
  const usable = (layer: Layer) => layer.visible && !layer.locked;
  const active = layers.find((layer) => layer.id === activeLayerId);
  if (active && usable(active)) return active;
  return [...layers].sort((a, b) => b.zIndex - a.zIndex).find(usable) ?? null;
}

const ShapeView = memo(function ShapeView({
  shape,
  geometry,
  onPointerDown,
}: {
  shape: RenderShape;
  geometry: ShapeGeometry;
  onPointerDown?: (event: PointerEvent) => void;
}) {
  const { x, y, width, height, rotation } = geometry;
  const transform = rotation ? `rotate(${rotation} ${x + width / 2} ${y + height / 2})` : undefined;
  const common = {
    transform,
    onPointerDown,
    className: onPointerDown ? "shape shape-editable" : "shape",
    "data-shape-id": shape.id,
    "data-kind": shape.kind,
  };
  const fill = fillColor(shape.fill);
  switch (shape.kind) {
    case "ellipse":
      return (
        <ellipse
          {...common}
          cx={x + width / 2}
          cy={y + height / 2}
          rx={width / 2}
          ry={height / 2}
          fill={fill}
        />
      );
    case "text":
      return (
        <g {...common}>
          <rect x={x} y={y} width={width} height={height} fill="transparent" />
          <text
            x={x}
            y={y + height * 0.78}
            fontSize={height * 0.82}
            fill={fill}
            className="poster-text"
          >
            {shape.text ?? ""}
          </text>
        </g>
      );
    case "image":
      return <ImageShape common={common} shape={shape} geometry={geometry} />;
    default:
      return <rect {...common} x={x} y={y} width={width} height={height} fill={fill} />;
  }
});

function ImageShape({
  common,
  shape,
  geometry,
}: {
  common: Record<string, unknown>;
  shape: RenderShape;
  geometry: ShapeGeometry;
}) {
  const url = useAssetUrlById(shape.assetId);
  const { x, y, width, height } = geometry;
  return (
    <g {...common}>
      <rect x={x} y={y} width={width} height={height} fill={fillColor("paper")} />
      {url && (
        <image
          href={url}
          x={x}
          y={y}
          width={width}
          height={height}
          preserveAspectRatio="xMidYMid slice"
        />
      )}
    </g>
  );
}

function SelectionFrame({
  geometry,
  handleSize,
  onHandleDown,
}: {
  geometry: ShapeGeometry;
  handleSize: number;
  onHandleDown: (event: PointerEvent, corner: Corner) => void;
}) {
  const { x, y, width, height, rotation } = geometry;
  const corners: [Corner, number, number][] = [
    ["nw", x, y],
    ["ne", x + width, y],
    ["sw", x, y + height],
    ["se", x + width, y + height],
  ];
  return (
    <g
      className="selection"
      transform={rotation ? `rotate(${rotation} ${x + width / 2} ${y + height / 2})` : undefined}
    >
      <rect x={x} y={y} width={width} height={height} className="selection-outline" />
      {corners.map(([corner, cx, cy]) => (
        <rect
          key={corner}
          className={`selection-handle selection-handle-${corner}`}
          x={cx - handleSize / 2}
          y={cy - handleSize / 2}
          width={handleSize}
          height={handleSize}
          onPointerDown={(event) => onHandleDown(event, corner)}
          aria-label={`Resize from ${corner} corner`}
        />
      ))}
    </g>
  );
}

/** Poster units per CSS pixel, so handles and cursor labels keep a screen size. */
function useUnitsPerPixel(ref: React.RefObject<SVGSVGElement | null>, posterWidth: number) {
  const [ratio, setRatio] = useState(1);
  useEffect(() => {
    const svg = ref.current;
    if (!svg) return;
    const measure = () => {
      const rendered = svg.getBoundingClientRect().width;
      if (rendered > 0) setRatio(posterWidth / rendered);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(svg);
    return () => observer.disconnect();
  }, [ref, posterWidth]);
  return ratio;
}

/**
 * Coalesce drag writes to one `db.update` per animation frame. Updates are
 * local-first, so collaborators see the drag live without flooding sync.
 */
function useFrameWriter(write: (id: string, geometry: ShapeGeometry) => void) {
  const pending = useRef<{ id: string; geometry: ShapeGeometry } | null>(null);
  const frame = useRef<number | null>(null);
  const writeRef = useRef(write);
  writeRef.current = write;
  const flush = useCallback(() => {
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = null;
    const next = pending.current;
    pending.current = null;
    if (next) writeRef.current(next.id, next.geometry);
  }, []);
  const schedule = useCallback(
    (id: string, geometry: ShapeGeometry) => {
      pending.current = { id, geometry };
      frame.current ??= requestAnimationFrame(flush);
    },
    [flush],
  );
  useEffect(() => flush, [flush]);
  return { schedule, flush };
}
