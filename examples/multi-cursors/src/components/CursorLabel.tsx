import { animated, to, useSpring } from "@react-spring/web";
import { useEffect, useRef, useState } from "react";
import { Vec2, ViewBox } from "../types";
import { getLabelPosition } from "../utils/getLabelPosition";

const DEBUG = import.meta.env.VITE_DEBUG === "true";

interface CursorLabelProps {
  name: string;
  color: string;
  position: Vec2;
  bounds?: ViewBox;
  isOutOfBounds?: boolean;
}

interface TextDimensions {
  width: number;
  height: number;
}

export function CursorLabel({
  name,
  color,
  position,
  bounds,
  isOutOfBounds,
}: CursorLabelProps) {
  const textRef = useRef<SVGTextElement>(null);
  const [dimensions, setDimensions] = useState<TextDimensions>({
    width: 0,
    height: 0,
  });

  useEffect(() => {
    const bbox = textRef.current?.getBBox();
    setDimensions({ width: bbox?.width ?? 0, height: bbox?.height ?? 0 });
  }, [name]);

  const labelPosition = getLabelPosition(
    position,
    dimensions,
    bounds,
    isOutOfBounds,
  );
  const labelSprings = useSpring<Vec2>({
    ...labelPosition,
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
      {/* @ts-expect-error - TODO: invalid after the React 19 upgrade */}
      <animated.text
        ref={textRef}
        x={to([labelSprings.x], (x) => x)}
        y={to([labelSprings.y], (y) => y)}
        fill={color}
        stroke="white"
        strokeWidth="3"
        strokeLinejoin="round"
        paintOrder="stroke"
        fontSize="14"
        dominantBaseline="hanging"
        textAnchor="start"
      >
        {name}
      </animated.text>
      {DEBUG ? (
        <>
          <text x={position.x} y={position.y} fill="red" fontSize="8">
            {position.x}, {position.y}
          </text>
          <text x={labelPosition.x} y={labelPosition.y} fill="red" fontSize="8">
            {bounds
              ? `${bounds.x - labelPosition.x}, ${bounds.y - labelPosition.y}`
              : "no bounds"}
          </text>
          <line
            x1={position.x}
            y1={position.y}
            x2={labelPosition.x}
            y2={labelPosition.y}
            stroke="red"
            strokeWidth="1"
            strokeLinejoin="round"
          />
        </>
      ) : null}
    </>
  );
}
