import { UserCursor } from "./UserCursor";
import { CanvasDemoContent } from "./CanvasDemoContent";
import { CanvasBackground } from "./CanvasBackground";
import { useCanvas } from "../hooks/useCanvas";

interface UserCursor {
  id: string;
  position: { x: number; y: number };
  color: string;
  isDragging: boolean;
}

interface CanvasProps {
  userCursors: UserCursor[];
}

function Canvas({ userCursors }: CanvasProps) {
  const {
    svgProps,
    isDragging,
    isMouseOver,
    mousePosition,
    bgPosition,
    dottedGridSize,
  } = useCanvas();

  return (
    <svg width="100%" height="100%" {...svgProps}>
      <CanvasBackground
        bgPosition={bgPosition}
        dottedGridSize={dottedGridSize}
      />

      <CanvasDemoContent />

      {userCursors.map((cursor) => (
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
