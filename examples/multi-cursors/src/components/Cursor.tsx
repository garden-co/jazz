import { animated, to, useSpring } from "@react-spring/web";
import { Vec2, ViewBox } from "../types";
import { calculateBoundaryIntersection } from "../utils/boundaryIntersection";
import { isOutOfBounds } from "../utils/isOutOfBounds";

interface CursorProps {
  position: { x: number; y: number };
  color: string;
  isDragging: boolean;
  isRemote: boolean;
  name: string;
  age?: number;
  centerOfBounds: Vec2;
  bounds?: ViewBox;
}

export function Cursor({
  position,
  color,
  isDragging,
  isRemote,
  name,
  age = 0,
  centerOfBounds,
  bounds,
}: CursorProps) {
  if (!bounds) {
    console.log("Boundless!");
    return null;
  }

  const intersectionPoint = calculateBoundaryIntersection(
    centerOfBounds,
    position,
    bounds,
  );

  const springs = useSpring({
    x: position.x,
    y: position.y,
    opacity: age > 60000 ? 0 : 1,
    immediate: !isRemote,
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
      <animated.g
        transform={to(
          [springs.x, springs.y],
          (x: number, y: number) => `translate(${x}, ${y})`,
        )}
        // style={{
        //   opacity: age > 30000 ? 0 : age > 10000 ? 0.5 : 1,
        //   transition: "opacity 0.3s ease-in-out",
        // }}
      >
        <polygon
          points="0,0 0,20 14.3,14.3"
          fill={
            isDragging
              ? color
              : `color-mix(in oklch, ${color}, transparent 56%)`
          }
          stroke={color}
          strokeWidth="3"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <text
          x="10"
          y="25"
          fill={color}
          stroke="white"
          strokeWidth="3"
          strokeLinejoin="round"
          paintOrder="stroke"
          fontSize="14"
          dominantBaseline="hanging"
          style={{
            fontFamily: "Inter, Manrope, system-ui, sans-serif",
            fontWeight: 500,
          }}
        >
          {name}
        </text>
      </animated.g>
      {isOutOfBounds(position, bounds) && (
        <circle
          cx={intersectionPoint.x}
          cy={intersectionPoint.y}
          r={4}
          fill={color}
        />
      )}
    </>
  );
}
