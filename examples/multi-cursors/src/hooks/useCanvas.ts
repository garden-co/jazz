import { useEffect, useState } from "react";

export function useCanvas() {
  const [viewBox, setViewBox] = useState({
    x: 0,
    y: 0,
    width: window.innerWidth,
    height: window.innerHeight,
  });
  const [isDragging, setIsDragging] = useState(false);
  const [isMouseOver, setIsMouseOver] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

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
  };
  const handleMouseUp = () => setIsDragging(false);
  const handleMouseEnter = () => setIsMouseOver(true);
  const handleMouseLeave = () => {
    setIsMouseOver(false);
    setIsDragging(false);
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget;
    const ctm = svg.getScreenCTM();

    if (!ctm) return;

    const point = svg.createSVGPoint();
    point.x = e.clientX;
    point.y = e.clientY;
    const svgPoint = point.matrixTransform(ctm.inverse());

    setMousePosition(svgPoint);

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
  };
}
