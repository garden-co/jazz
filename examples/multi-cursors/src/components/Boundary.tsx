import { ViewBox } from "../types";

export function Boundary({ bounds }: { bounds: ViewBox }) {
  return (
    <>
      <rect
        x={bounds.x}
        y={bounds.y}
        width={bounds.width}
        height={bounds.height}
        stroke="red"
        fill="none"
      />
    </>
  );
}
