import { animated, to, useSpring } from "@react-spring/web";
import { useEffect, useRef, useState } from "react";
import { Vec2, ViewBox } from "../types";

const DEBUG = import.meta.env.VITE_DEBUG === "true";

interface CursorLabelProps {
  name: string;
  color: string;
  position: Vec2;
  bounds?: ViewBox;
  isOutOfBounds?: boolean;
}

interface LabelPosition {
  x: number;
  y: number;
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

  const getLabelPosition = (): LabelPosition => {
    if (!isOutOfBounds || !bounds) {
      return { x: position.x + 15, y: position.y + 25 };
    }

    // Calculate the percentage of the bounds that the intersection point is from the left
    const percentageH = (position.x - bounds.x) / bounds.width;
    const percentageV = (position.y - bounds.y) / bounds.height;

    return {
      x: position.x - percentageH * dimensions.width,
      y: position.y - percentageV * dimensions.height,
    };
  };

  const labelSprings = useSpring<LabelPosition>({
    ...getLabelPosition(),
    config: {
      tension: 170,
      friction: 26,
    },
  });

  return (
    <>
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
          <text
            x={getLabelPosition().x}
            y={getLabelPosition().y}
            fill="red"
            fontSize="8"
          >
            {bounds?.x - getLabelPosition().x},{" "}
            {bounds?.y - getLabelPosition().y}
          </text>
          <line
            x1={position.x}
            y1={position.y}
            x2={getLabelPosition().x}
            y2={getLabelPosition().y}
            stroke="red"
            strokeWidth="1"
            strokeLinejoin="round"
          />
        </>
      ) : null}
    </>
  );
}
