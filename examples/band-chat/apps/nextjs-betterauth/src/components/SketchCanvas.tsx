"use client";

import { useRef, useState, type PointerEvent } from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Button, HStack, Text, ToggleButton, VStack } from "@astryxdesign/core";
import { app } from "../../schema";

// Stroke colours are stored as names and drawn with design tokens, so a
// sketch follows light and dark mode.
const PENS = {
  ink: "var(--color-text-primary)",
  blue: "var(--color-data-categorical-blue)",
  red: "var(--color-data-categorical-red)",
  green: "var(--color-data-categorical-green)",
  orange: "var(--color-data-categorical-orange)",
} as const;
type Pen = keyof typeof PENS;
const PEN_NAMES: Record<Pen, string> = {
  ink: "Ink",
  blue: "Blue",
  red: "Red",
  green: "Green",
  orange: "Orange",
};
const WIDTH = 1000;
const HEIGHT = 625;
const STROKE_WIDTH = 6;

function toPath(points: readonly number[]): string {
  if (points.length < 2) return "";
  let path = `M${points[0]} ${points[1]}`;
  for (let index = 2; index + 1 < points.length; index += 2)
    path += `L${points[index]} ${points[index + 1]}`;
  // A single tap still leaves a dot.
  if (points.length === 2) path += `L${points[0]} ${points[1]}`;
  return path;
}

/**
 * A shared sketch attached to a message. Each finished stroke is one row, so
 * bandmates see strokes appear as they are drawn and offline strokes sync later.
 */
export function SketchCanvas({
  canvasId,
  roomId,
  author,
}: {
  canvasId: string;
  roomId: string;
  author: string;
}) {
  const db = useDb();
  const svg = useRef<SVGSVGElement>(null);
  const { data: strokes = [] } = useAll(
    app.strokes.where({ canvasId }).select("*", "$createdAt").orderBy("$createdAt", "asc"),
  );
  const [pen, setPen] = useState<Pen>("ink");
  const [draft, setDraft] = useState<number[] | null>(null);
  const mine = strokes.filter((stroke) => stroke.author === author);

  function point(event: PointerEvent<SVGSVGElement>): [number, number] {
    const box = svg.current!.getBoundingClientRect();
    return [
      Math.round(((event.clientX - box.left) / box.width) * WIDTH),
      Math.round(((event.clientY - box.top) / box.height) * HEIGHT),
    ];
  }

  function finish() {
    if (draft && draft.length >= 2) {
      db.insert(app.strokes, {
        canvasId,
        roomId,
        author,
        color: pen,
        width: STROKE_WIDTH,
        points: draft,
      });
    }
    setDraft(null);
  }

  return (
    <VStack gap={2} className="sketch">
      <svg
        ref={svg}
        className="sketch-surface"
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`Shared sketch with ${strokes.length} strokes`}
        onPointerDown={(event) => {
          event.currentTarget.setPointerCapture(event.pointerId);
          setDraft(point(event));
        }}
        onPointerMove={(event) => {
          if (!draft) return;
          const [x, y] = point(event);
          const lastX = draft[draft.length - 2]!;
          const lastY = draft[draft.length - 1]!;
          if (Math.hypot(x - lastX, y - lastY) >= 4) setDraft([...draft, x, y]);
        }}
        onPointerUp={finish}
        onPointerCancel={() => setDraft(null)}
      >
        {strokes.map((stroke) => (
          <path
            key={stroke.id}
            d={toPath(stroke.points)}
            stroke={PENS[stroke.color as Pen] ?? PENS.ink}
            strokeWidth={stroke.width}
          />
        ))}
        {draft ? <path d={toPath(draft)} stroke={PENS[pen]} strokeWidth={STROKE_WIDTH} /> : null}
      </svg>
      <HStack gap={1} wrap="wrap" vAlign="center">
        <Text type="supporting" color="secondary">
          Pen
        </Text>
        {(Object.keys(PENS) as Pen[]).map((name) => (
          <ToggleButton
            key={name}
            size="sm"
            label={PEN_NAMES[name]}
            isPressed={pen === name}
            onPressedChange={() => setPen(name)}
            icon={
              <svg viewBox="0 0 10 10" width="10" height="10" aria-hidden="true">
                <circle cx="5" cy="5" r="5" fill={PENS[name]} />
              </svg>
            }
          />
        ))}
        <Button
          label="Undo"
          size="sm"
          variant="ghost"
          isDisabled={mine.length === 0}
          onClick={() => db.delete(app.strokes, mine[mine.length - 1]!.id)}
        />
      </HStack>
    </VStack>
  );
}
