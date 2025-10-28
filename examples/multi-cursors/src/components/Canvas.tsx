import { CoFeedEntry } from "jazz-tools";
import { CursorMoveEvent, useCanvas } from "../hooks/useCanvas";
import { Cursor as CursorType, Vec2, ViewBox } from "../types";
import { centerOfBounds } from "../utils/centerOfBounds";
import { Boundary } from "./Boundary";
import { CanvasBackground } from "./CanvasBackground";
import { CanvasDemoContent } from "./CanvasDemoContent";
import { Cursor } from "./Cursor";
const DEBUG = import.meta.env.VITE_DEBUG === "true";

// For debugging purposes, we can set a fixed bounds
const debugBounds: ViewBox = {
  x: 320,
  y: 320,
  width: 640,
  height: 640,
};

interface CanvasProps {
  remoteCursors: {
    entry: CoFeedEntry<CursorType>;
    position: Vec2;
    color: string;
    name: string;
    age: number;
    isMe?: boolean;
    active?: boolean;
  }[];
  onCursorMove: (move: CursorMoveEvent) => void;
  name: string;
}

function Canvas({ remoteCursors, onCursorMove, name }: CanvasProps) {
  const {
    svgProps,
    isDragging,
    isMouseOver,
    mousePosition,
    bgPosition,
    dottedGridSize,
    viewBox,
  } = useCanvas({ onCursorMove });

  const bounds = DEBUG ? debugBounds : viewBox;
  const center = centerOfBounds(bounds);

  return (
    <svg width="100%" height="100%" {...svgProps}>
      <CanvasBackground
        bgPosition={bgPosition}
        dottedGridSize={dottedGridSize}
      />

      <CanvasDemoContent />
      {DEBUG && <Boundary bounds={bounds} />}

      {remoteCursors.map((cursor) =>
        !cursor.isMe && cursor.active ? (
          <Cursor
            key={cursor.entry.tx.sessionID}
            position={cursor.position}
            color={cursor.color}
            isDragging={false}
            isRemote={true}
            name={cursor.name}
            age={cursor.age}
            centerOfBounds={center}
            bounds={bounds}
          />
        ) : null,
      )}

      {isMouseOver ? (
        <Cursor
          position={mousePosition}
          color="#FF69B4"
          isDragging={isDragging}
          isRemote={false}
          name={name}
          centerOfBounds={center}
          bounds={bounds}
        />
      ) : null}
    </svg>
  );
}

export default Canvas;
