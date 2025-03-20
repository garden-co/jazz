import { UserCursor } from "./UserCursor";
import { CanvasDemoContent } from "./CanvasDemoContent";
import { CanvasBackground } from "./CanvasBackground";
import { CursorMoveEvent, useCanvas } from "../hooks/useCanvas";

interface UserCursor {
  id: string;
  position: { x: number; y: number };
  color: string;
  isDragging: boolean;
}

interface CanvasProps {
  remoteCursors: UserCursor[];
  onCursorMove: (move: CursorMoveEvent) => void;
}

function Canvas({ remoteCursors, onCursorMove }: CanvasProps) {
  const {
    svgProps,
    isDragging,
    isMouseOver,
    mousePosition,
    bgPosition,
    dottedGridSize,
  } = useCanvas({ onCursorMove });

  return (
    <svg width="100%" height="100%" {...svgProps}>
      <CanvasBackground
        bgPosition={bgPosition}
        dottedGridSize={dottedGridSize}
      />

      <CanvasDemoContent />

      {remoteCursors.map((cursor) => (
        <UserCursor
          key={cursor.id}
          position={cursor.position}
          color={cursor.color}
          isDragging={cursor.isDragging}
        />
      ))}

      {isMouseOver ? (
        <UserCursor
          position={mousePosition}
          color="#FF69B4"
          isDragging={isDragging}
        />
      ) : null}
    </svg>
  );
}

export default Canvas;
