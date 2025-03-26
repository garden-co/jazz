import { Vec2, ViewBox } from "../types";

interface OutOfBoundsMarkerProps {
  position: Vec2;
  bounds: ViewBox;
}

function middleOfBounds(bounds?: ViewBox): Vec2 {
  if (!bounds) {
    return { x: 0, y: 0 };
  }

  return {
    x: bounds.x + bounds.width / 2,
    y: bounds.y + bounds.height / 2,
  };
}

function OutOfBoundsMarker({ position, bounds }: OutOfBoundsMarkerProps) {
  return (
    <>
      <rect x={position.x} y={position.y} width={4} height={4} fill="red" />
      <line
        x1={position.x}
        y1={position.y}
        x2={middleOfBounds(bounds).x}
        y2={middleOfBounds(bounds).y}
        stroke="red"
      />
    </>
  );
}

export { OutOfBoundsMarker };
