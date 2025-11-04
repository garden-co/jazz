"use client";

import { useLayoutEffect, useState } from "react";
import { pingColorThresholds } from "./pingColorThresholds";

export default function MapTooltip() {
  const [style, setStyle] = useState<React.CSSProperties>({});
  const [circleStyle, setCircleStyle] = useState<React.CSSProperties>({});
  const [text, setText] = useState("");

  useLayoutEffect(() => {
    const onSvgMouseMove = (e: MessageEvent) => {
      if (e.data.type === "svgmouseout") {
        setStyle({ display: "none" });
        return;
      }
      if (e.data.type !== "svgmouseover") return;

      const x = e.data.x;
      const y = e.data.y;
      const ping = e.data.ping;
      const via = e.data.via;
      const to = e.data.to;
      const text = `${ping}ms via ${via} to ${to}`;

      setStyle({
        display: "flex",
        left: `calc(100% * ${x / 1400} + 30px)`,
        top: `calc(100% * ${(y || 0) / 440} + 15px)`,
      });

      const threshold = pingColorThresholds.find((t) => t.ping >= ping);
      if (threshold) {
        setCircleStyle({
          backgroundColor: `light-dark(${threshold.fill}, ${threshold.darkFill})`,
        });
      }
      setText(text);
    };

    window.addEventListener("message", onSvgMouseMove);

    return () => {
      window.removeEventListener("message", onSvgMouseMove);
    };
  }, []);

  return (
    <>
      <iframe
        className="aspect-12/4 w-full dark:hidden"
        src="/api/latencyMap?spacing=1.5&dark=false&mouse=true"
        title="Interactive latency map light mode"
      />
      <iframe
        className="aspect-12/4 hidden w-full dark:block"
        src="/api/latencyMap?spacing=1.5&dark=true&mouse=true"
        style={{ colorScheme: "light" }}
        title="Interactive latency map dark mode"
      />
      <div
        className="map-tooltip pointer-events-none absolute hidden items-center gap-1 rounded-lg bg-stone-925 p-2 text-xs text-stone-50"
        style={style}
      >
        <div className="h-3 w-3 rounded-full" style={circleStyle}></div>
        <div className="text-xs">{text}</div>
      </div>
    </>
  );
}
