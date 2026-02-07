import { useEffect } from "react";

export function useResponsiveCanvas(
  containerRef: React.RefObject<HTMLDivElement | null>,
  canvasRefs: React.RefObject<Map<string, HTMLCanvasElement | null>>,
  onResize?: () => void,
) {
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;

      const { width, height } = entry.contentRect;
      const dpr = window.devicePixelRatio || 1;

      const physicalWidth = Math.floor(width * dpr);
      const physicalHeight = Math.floor(height * dpr);

      let hasChanged = false;

      for (const canvas of canvasRefs.current.values()) {
        if (!canvas) continue;
        if (
          canvas.width !== physicalWidth ||
          canvas.height !== physicalHeight
        ) {
          canvas.width = physicalWidth;
          canvas.height = physicalHeight;
          hasChanged = true;
        }
      }

      if (hasChanged && onResize) {
        onResize();
      }
    });

    observer.observe(container);
    return () => observer.disconnect();
  }, [containerRef, canvasRefs, onResize]);
}
