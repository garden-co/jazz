import { useCallback, useEffect, useState } from "react";
import type { ViewBox } from "../types";
import { throttleTime } from "../utils/throttleTime";

export interface CursorMoveEvent {
  position: { x: number; y: number };
  isDragging: boolean;
}

export function useCanvas({
  onCursorMove,
  throttleMs = 100,
}: {
  onCursorMove: (event: CursorMoveEvent) => void;
  throttleMs?: number;
}) {
  const [viewBox, setViewBox] = useState<ViewBox>({
    x: 0,
    y: 0,
    width: window.innerWidth,
    height: window.innerHeight,
  });
  const [isDragging, setIsDragging] = useState(false);
  const [isMouseOver, setIsMouseOver] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

  const onCursorMoveThrottled = useCallback(
    throttleTime((move: CursorMoveEvent) => onCursorMove(move), throttleMs),
    [onCursorMove, throttleMs],
  );

  useEffect(() => {
    const handleResize = () => {
      setViewBox((prev) => ({
        ...prev,
        width: window.innerWidth,
        height: window.innerHeight,
      }));
    };

    setViewBox((prev) => ({
      ...prev,
      x: -window.innerWidth / 2,
      y: -window.innerHeight / 2,
    }));

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const handleMouseDown = (e: React.MouseEvent<SVGSVGElement>) => {
    setIsDragging(true);
    setDragStart({
      x: e.clientX,
      y: e.clientY,
    });

    onCursorMoveThrottled({
      position: mousePosition,
      isDragging: true,
    });
  };
  const handleMouseUp = () => {
    setIsDragging(false);

    onCursorMoveThrottled({
      position: mousePosition,
      isDragging: false,
    });
  };
  const handleMouseEnter = () => setIsMouseOver(true);
  const handleMouseLeave = () => {
    setIsMouseOver(false);
    setIsDragging(false);

    onCursorMoveThrottled({
      position: mousePosition,
      isDragging: false,
    });
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget;
    const ctm = svg.getScreenCTM();

    if (!ctm) throw new Error("can't get SVG screen CTM");

    const point = svg.createSVGPoint();
    point.x = e.clientX;
    point.y = e.clientY;
    const svgPoint = point.matrixTransform(ctm.inverse());

    setMousePosition(svgPoint);

    onCursorMoveThrottled({
      position: svgPoint,
      isDragging,
    });

    if (!isDragging) return;

    const dx = e.clientX - dragStart.x;
    const dy = e.clientY - dragStart.y;

    setViewBox((prev) => ({ ...prev, x: prev.x - dx, y: prev.y - dy }));
    setDragStart({ x: e.clientX, y: e.clientY });
  };

  const dottedGridSize = 40;

  const bgPosition = {
    x: Math.floor(viewBox.x / dottedGridSize) * dottedGridSize,
    y: Math.floor(viewBox.y / dottedGridSize) * dottedGridSize,
  };

  const handlers = {
    onMouseDown: handleMouseDown,
    onMouseUp: handleMouseUp,
    onMouseEnter: handleMouseEnter,
    onMouseLeave: handleMouseLeave,
    onMouseMove: handleMouseMove,
  };

  const svgProps: React.SVGProps<SVGSVGElement> = {
    ...handlers,
    viewBox: `${viewBox.x} ${viewBox.y} ${viewBox.width} ${viewBox.height}`,
    className: "select-none cursor-none",
  };

  return {
    svgProps,
    isDragging,
    isMouseOver,
    mousePosition,
    bgPosition,
    dottedGridSize,
    viewBox,
  };
}
