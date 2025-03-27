import { useAccount } from "jazz-react";
import { CoFeedEntry, co } from "jazz-tools";
import { CursorMoveEvent, useCanvas } from "../hooks/useCanvas";
import { Cursor as CursorType, ViewBox } from "../types";
import { centerOfBounds } from "../utils/centerOfBounds";
import { getColor } from "../utils/getColor";
import { getName } from "../utils/getName";
import { Boundary } from "./Boundary";
import { CanvasBackground } from "./CanvasBackground";
import { CanvasDemoContent } from "./CanvasDemoContent";
import { Cursor } from "./Cursor";

const OLD_CURSOR_AGE_SECONDS = Number(
  import.meta.env.VITE_OLD_CURSOR_AGE_SECONDS,
);

// For debugging purposes, we can set a fixed bounds
const bounds: ViewBox = {
  x: -320,
  y: -320,
  width: 640,
  height: 640,
};

interface CanvasProps {
  remoteCursors: CoFeedEntry<co<CursorType>>[];
  onCursorMove: (move: CursorMoveEvent) => void;
  name: string;
}

function Canvas({ remoteCursors, onCursorMove, name }: CanvasProps) {
  const { me } = useAccount();

  const {
    svgProps,
    isDragging,
    isMouseOver,
    mousePosition,
    bgPosition,
    dottedGridSize,
    // viewBox: bounds,
  } = useCanvas({ onCursorMove });

  const center = centerOfBounds(bounds);

  return (
    <svg width="100%" height="100%" {...svgProps}>
      <CanvasBackground
        bgPosition={bgPosition}
        dottedGridSize={dottedGridSize}
      />

      <CanvasDemoContent />
      <Boundary bounds={bounds} />

      {remoteCursors.map((entry) => {
        if (
          entry.tx.sessionID === me?.sessionID ||
          (OLD_CURSOR_AGE_SECONDS &&
            entry.madeAt < new Date(Date.now() - 1000 * OLD_CURSOR_AGE_SECONDS))
        ) {
          return null;
        }

        const name = getName(entry.by?.profile?.name, entry.tx.sessionID);
        const color = getColor(entry.tx.sessionID);
        const age = new Date().getTime() - new Date(entry.madeAt).getTime();

        return (
          <>
            <Cursor
              key={entry.tx.sessionID}
              position={entry.value.position}
              color={color}
              isDragging={false}
              isRemote={true}
              name={name}
              age={age}
              centerOfBounds={center}
              bounds={bounds}
            />
          </>
        );
      })}

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
