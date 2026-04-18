"use client";

import { startTransition, useEffect, useRef, useState } from "react";
import { collectSharpTurnPausePoints, type PlotPoint } from "./svg-plotter-geometry";
import {
  buildFilteredSvg,
  buildOriginalSvg,
  type PauseDot,
  type PlotterSettings,
  type PreparedSvg,
} from "./svg-plotter-render";
import { getEmbeddedSvgStyleValue, parseEmbeddedSvgStyles } from "./svg-plotter-style";

type SliderConfig = {
  description: string;
  key: keyof PlotterSettings;
  label: string;
  max: number;
  min: number;
  step: number;
};

const defaultSettings: PlotterSettings = {
  bleed: 0.22,
  endLinger: 0.18,
  frequency: 0.0155,
  opacity: 0.88,
  tipWidth: 0.02,
  wobble: 1,
};

const sliderConfigs: SliderConfig[] = [
  {
    key: "wobble",
    label: "Pen wobble",
    min: 0,
    max: 10,
    step: 0.1,
    description: "How much the line walks while staying mechanically precise.",
  },
  {
    key: "frequency",
    label: "Micro jitter",
    min: 0.001,
    max: 0.02,
    step: 0.0005,
    description: "The scale of the imperfection pattern along the path.",
  },
  {
    key: "tipWidth",
    label: "Tip width",
    min: 0,
    max: 1.2,
    step: 0.02,
    description: "How much the felt tip slightly fattens the mark.",
  },
  {
    key: "bleed",
    label: "Ink bleed",
    min: 0,
    max: 1.2,
    step: 0.02,
    description: "Softens the edge like a Stabilo on slightly toothy paper.",
  },
  {
    key: "endLinger",
    label: "Pen linger",
    min: 0,
    max: 1,
    step: 0.01,
    description:
      "Adds a tiny pause at open ends and sharp turns so the pen pools very slightly there.",
  },
  {
    key: "opacity",
    label: "Ink density",
    min: 0.55,
    max: 1,
    step: 0.01,
    description: "Overall density of the pen mark.",
  },
];

function parseSvgLength(value: string | null) {
  if (!value) return null;

  const match = value.match(/-?\d*\.?\d+/);
  if (!match) return null;

  const parsed = Number.parseFloat(match[0]);

  return Number.isFinite(parsed) ? parsed : null;
}

function parseStyleValue(style: string | null, property: string) {
  if (!style) return null;

  const match = style.match(new RegExp(`${property}\\s*:\\s*([^;]+)`, "i"));
  return match?.[1]?.trim() ?? null;
}

function getPresentationalValue(
  element: Element,
  attribute: string,
  embeddedStyles?: ReturnType<typeof parseEmbeddedSvgStyles>,
) {
  return (
    element.getAttribute(attribute) ??
    parseStyleValue(element.getAttribute("style"), attribute) ??
    (embeddedStyles
      ? getEmbeddedSvgStyleValue(
          embeddedStyles,
          {
            classNames: element.getAttribute("class")?.split(/\s+/).filter(Boolean),
            id: element.getAttribute("id"),
            tagName: element.tagName,
          },
          attribute,
        )
      : null)
  );
}

function distance(a: { x: number; y: number }, b: { x: number; y: number }) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function normalizeVector(x: number, y: number) {
  const length = Math.hypot(x, y);
  if (length < 0.0001) {
    return { x: 0, y: 0 };
  }

  return {
    x: x / length,
    y: y / length,
  };
}

function addPauseDot(dots: PauseDot[], dot: PauseDot) {
  const minDistance = Math.max(1.5, dot.strokeWidth * 0.9);
  const existingIndex = dots.findIndex(
    (existing) =>
      existing.color === dot.color &&
      distance({ x: existing.cx, y: existing.cy }, { x: dot.cx, y: dot.cy }) < minDistance,
  );

  if (existingIndex === -1) {
    dots.push(dot);
    return;
  }

  if (dots[existingIndex]!.lingerStrength < dot.lingerStrength) {
    dots[existingIndex] = dot;
  }
}

function samplePathPoints(element: SVGPathElement, totalLength: number, strokeWidth: number) {
  const sampleSpacing = Math.max(6, strokeWidth * 4);
  const segmentCount = Math.max(8, Math.min(600, Math.ceil(totalLength / sampleSpacing)));
  const points: PlotPoint[] = [];

  for (let index = 0; index <= segmentCount; index += 1) {
    const point = element.getPointAtLength((totalLength * index) / segmentCount);
    const normalizedPoint = { x: point.x, y: point.y };

    if (points.length === 0 || distance(points.at(-1)!, normalizedPoint) > 0.35) {
      points.push(normalizedPoint);
    }
  }

  return points;
}

function getSharpTurnOptions(strokeWidth: number, closed = false) {
  return {
    closed,
    minSegmentLength: Math.max(1.5, strokeWidth * 0.65),
    minSpacing: Math.max(3, strokeWidth * 1.1),
    minTurnAngleDegrees: closed ? 24 : 28,
  };
}

function addSharpTurnPauseDots(
  dots: PauseDot[],
  points: PlotPoint[],
  stroke: string,
  strokeWidth: number,
  closed = false,
) {
  for (const pausePoint of collectSharpTurnPausePoints(
    points,
    getSharpTurnOptions(strokeWidth, closed),
  )) {
    addPauseDot(dots, {
      color: stroke,
      cx: pausePoint.x,
      cy: pausePoint.y,
      lingerStrength: pausePoint.lingerStrength,
      strokeWidth,
      trailX: pausePoint.trailX,
      trailY: pausePoint.trailY,
    });
  }
}

function sanitizeSvg(root: SVGSVGElement) {
  root.querySelectorAll("script, foreignObject").forEach((element) => element.remove());

  for (const element of root.querySelectorAll("*")) {
    for (const attribute of [...element.getAttributeNames()]) {
      const lowerName = attribute.toLowerCase();
      const value = element.getAttribute(attribute) ?? "";

      if (lowerName.startsWith("on")) {
        element.removeAttribute(attribute);
        continue;
      }

      if ((lowerName === "href" || lowerName === "xlink:href") && value.startsWith("javascript:")) {
        element.removeAttribute(attribute);
      }
    }
  }
}

function getPauseDots(
  root: SVGSVGElement,
  embeddedStyles: ReturnType<typeof parseEmbeddedSvgStyles>,
): PauseDot[] {
  const dots: PauseDot[] = [];

  for (const element of root.querySelectorAll("line, polyline, polygon, rect, path")) {
    const stroke = getPresentationalValue(element, "stroke", embeddedStyles);
    if (!stroke || stroke === "none") continue;

    const strokeWidth =
      parseSvgLength(getPresentationalValue(element, "stroke-width", embeddedStyles)) ?? 1;
    const markerStart = getPresentationalValue(element, "marker-start", embeddedStyles);
    const markerEnd = getPresentationalValue(element, "marker-end", embeddedStyles);

    if (element instanceof SVGLineElement) {
      if (!markerStart || markerStart === "none") {
        const trail = normalizeVector(
          element.x2.baseVal.value - element.x1.baseVal.value,
          element.y2.baseVal.value - element.y1.baseVal.value,
        );
        addPauseDot(dots, {
          color: stroke,
          cx: element.x1.baseVal.value,
          cy: element.y1.baseVal.value,
          lingerStrength: 1,
          strokeWidth,
          trailX: trail.x,
          trailY: trail.y,
        });
      }

      if (!markerEnd || markerEnd === "none") {
        const trail = normalizeVector(
          element.x1.baseVal.value - element.x2.baseVal.value,
          element.y1.baseVal.value - element.y2.baseVal.value,
        );
        addPauseDot(dots, {
          color: stroke,
          cx: element.x2.baseVal.value,
          cy: element.y2.baseVal.value,
          lingerStrength: 1,
          strokeWidth,
          trailX: trail.x,
          trailY: trail.y,
        });
      }

      continue;
    }

    if (element instanceof SVGPolylineElement) {
      const points = Array.from(element.points).map((point) => ({ x: point.x, y: point.y }));
      if (points.length < 2) continue;

      if (!markerStart || markerStart === "none") {
        const trail = normalizeVector(points[1]!.x - points[0]!.x, points[1]!.y - points[0]!.y);
        addPauseDot(dots, {
          color: stroke,
          cx: points[0].x,
          cy: points[0].y,
          lingerStrength: 1,
          strokeWidth,
          trailX: trail.x,
          trailY: trail.y,
        });
      }

      if (!markerEnd || markerEnd === "none") {
        const trail = normalizeVector(
          points.at(-2)!.x - points.at(-1)!.x,
          points.at(-2)!.y - points.at(-1)!.y,
        );
        addPauseDot(dots, {
          color: stroke,
          cx: points.at(-1)!.x,
          cy: points.at(-1)!.y,
          lingerStrength: 1,
          strokeWidth,
          trailX: trail.x,
          trailY: trail.y,
        });
      }

      addSharpTurnPauseDots(dots, points, stroke, strokeWidth);

      continue;
    }

    if (element instanceof SVGPolygonElement) {
      const points = Array.from(element.points).map((point) => ({ x: point.x, y: point.y }));
      if (points.length < 3) continue;

      addSharpTurnPauseDots(dots, points, stroke, strokeWidth, true);
      continue;
    }

    if (element instanceof SVGRectElement) {
      const rx = element.rx.baseVal.value;
      const ry = element.ry.baseVal.value;
      if (rx > 0 || ry > 0) continue;

      const x = element.x.baseVal.value;
      const y = element.y.baseVal.value;
      const width = element.width.baseVal.value;
      const height = element.height.baseVal.value;

      if (width <= 0 || height <= 0) continue;

      addSharpTurnPauseDots(
        dots,
        [
          { x, y },
          { x: x + width, y },
          { x: x + width, y: y + height },
          { x, y: y + height },
        ],
        stroke,
        strokeWidth,
        true,
      );

      continue;
    }

    if (element instanceof SVGPathElement) {
      try {
        const totalLength = element.getTotalLength();
        if (!Number.isFinite(totalLength) || totalLength <= 0) continue;

        const start = element.getPointAtLength(0);
        const end = element.getPointAtLength(totalLength);
        const endpointSampleLength = Math.min(totalLength, Math.max(1, strokeWidth * 2.2));

        const isClosed = distance(start, end) < Math.max(0.5, strokeWidth * 0.2);

        if (!isClosed && (!markerStart || markerStart === "none")) {
          const startNext = element.getPointAtLength(endpointSampleLength);
          const trail = normalizeVector(startNext.x - start.x, startNext.y - start.y);
          addPauseDot(dots, {
            color: stroke,
            cx: start.x,
            cy: start.y,
            lingerStrength: 1,
            strokeWidth,
            trailX: trail.x,
            trailY: trail.y,
          });
        }

        if (!isClosed && (!markerEnd || markerEnd === "none")) {
          const endPrevious = element.getPointAtLength(
            Math.max(0, totalLength - endpointSampleLength),
          );
          const trail = normalizeVector(endPrevious.x - end.x, endPrevious.y - end.y);
          addPauseDot(dots, {
            color: stroke,
            cx: end.x,
            cy: end.y,
            lingerStrength: 1,
            strokeWidth,
            trailX: trail.x,
            trailY: trail.y,
          });
        }

        addSharpTurnPauseDots(
          dots,
          samplePathPoints(element, totalLength, strokeWidth),
          stroke,
          strokeWidth,
          isClosed,
        );
      } catch {
        continue;
      }
    }
  }

  return dots;
}

function prepareSvg(svgText: string): PreparedSvg {
  const parser = new DOMParser();
  const doc = parser.parseFromString(svgText, "image/svg+xml");
  const parserError = doc.querySelector("parsererror");

  if (parserError) {
    throw new Error("That file does not parse as valid SVG.");
  }

  const root = doc.querySelector("svg");

  if (!(root instanceof SVGSVGElement)) {
    throw new Error("Please drop an actual SVG root element.");
  }

  sanitizeSvg(root);
  const embeddedStyles = parseEmbeddedSvgStyles(
    Array.from(root.querySelectorAll("style"), (styleElement) => styleElement.textContent ?? ""),
  );

  const viewBox =
    root.getAttribute("viewBox") ??
    `0 0 ${parseSvgLength(root.getAttribute("width")) ?? 1200} ${
      parseSvgLength(root.getAttribute("height")) ?? 720
    }`;
  const preserveAspectRatio = root.getAttribute("preserveAspectRatio") ?? "xMidYMid meet";

  root.setAttribute("viewBox", viewBox);
  root.setAttribute("preserveAspectRatio", preserveAspectRatio);

  return {
    contentMarkup: root.innerHTML,
    pauseDots: getPauseDots(root, embeddedStyles),
    preserveAspectRatio,
    viewBox,
  };
}

function formatSliderValue(key: keyof PlotterSettings, value: number) {
  if (key === "frequency") return value.toFixed(4);
  if (key === "opacity" || key === "endLinger") return value.toFixed(2);
  return value.toFixed(2).replace(/\.00$/, "");
}

type SvgPlotterPlaygroundProps = {
  defaultFileName?: string;
  defaultSvg: string;
};

type PreviewMode = "original" | "plotted";

export function SvgPlotterPlayground({
  defaultFileName = "default-preview.svg",
  defaultSvg,
}: SvgPlotterPlaygroundProps) {
  const inputRef = useRef<HTMLInputElement | null>(null);

  const [fileName, setFileName] = useState(defaultFileName);
  const [isDragging, setIsDragging] = useState(false);
  const [parseError, setParseError] = useState<string | null>(null);
  const [prepared, setPrepared] = useState<PreparedSvg | null>(null);
  const [previewMode, setPreviewMode] = useState<PreviewMode>("plotted");
  const [rawSvg, setRawSvg] = useState(defaultSvg);
  const [seed, setSeed] = useState(11);
  const [settings, setSettings] = useState(defaultSettings);

  useEffect(() => {
    try {
      setPrepared(prepareSvg(rawSvg));
      setParseError(null);
    } catch (error) {
      setPrepared(null);
      setParseError(
        error instanceof Error ? error.message : "Something went wrong parsing the SVG.",
      );
    }
  }, [rawSvg]);

  const filteredMarkup = prepared ? buildFilteredSvg(prepared, settings, seed) : null;
  const originalMarkup = prepared ? buildOriginalSvg(prepared) : null;
  const previewMarkup = previewMode === "original" ? originalMarkup : filteredMarkup;

  async function loadFile(file: File) {
    if (file.type !== "image/svg+xml" && !file.name.toLowerCase().endsWith(".svg")) {
      setParseError("Please drop an SVG file so the filter can stay vector-based.");
      return;
    }

    const text = await file.text();

    startTransition(() => {
      setFileName(file.name);
      setRawSvg(text);
    });
  }

  function handleDownload() {
    if (!filteredMarkup) return;

    const blob = new Blob([filteredMarkup], { type: "image/svg+xml;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");

    link.href = url;
    link.download = fileName.replace(/\.svg$/i, "") + "-plotter.svg";
    link.click();

    URL.revokeObjectURL(url);
  }

  return (
    <div className="space-y-10">
      <div className="max-w-[44rem] space-y-4">
        <p className="text-sm leading-relaxed text-fd-muted-foreground">
          Drop in a line-heavy SVG and Jazz will render it through a slightly unruly pen filter:
          still machine-drawn, but with just enough wobble, overdraw, and bleed to feel plotted.
        </p>
        <div className="flex flex-wrap gap-3">
          <button
            type="button"
            onClick={() => inputRef.current?.click()}
            className="rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-card"
          >
            Choose SVG
          </button>
          <button
            type="button"
            onClick={() => {
              setFileName(defaultFileName);
              setRawSvg(defaultSvg);
            }}
            className="rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-card"
          >
            Reset sample
          </button>
          <button
            type="button"
            onClick={() => setSeed((current) => current + 1)}
            className="rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-card"
          >
            Re-roll noise
          </button>
          <button
            type="button"
            onClick={() => setSettings(defaultSettings)}
            className="rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-card"
          >
            Reset sliders
          </button>
          <button
            type="button"
            onClick={handleDownload}
            disabled={!filteredMarkup}
            className="rounded-full border border-fd-border px-4 py-2 text-sm font-medium transition-colors hover:bg-fd-card disabled:cursor-not-allowed disabled:opacity-50"
          >
            Download filtered SVG
          </button>
        </div>
        <input
          ref={inputRef}
          type="file"
          accept=".svg,image/svg+xml"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) void loadFile(file);
            event.currentTarget.value = "";
          }}
        />
      </div>

      {parseError ? (
        <div className="rounded-3xl border border-red-400/30 bg-red-500/6 p-5 text-sm text-red-900">
          {parseError}
        </div>
      ) : null}

      <div className="grid gap-6 lg:grid-cols-[22rem_minmax(0,1fr)] xl:grid-cols-[24rem_minmax(0,1fr)]">
        <aside className="space-y-6 rounded-[2rem] border border-fd-border bg-fd-card/50 p-6">
          <div className="space-y-3">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Controls
            </p>
            <p className="text-sm leading-relaxed text-fd-muted-foreground">
              The sweet spot is usually low wobble, a small amount of overdraw, and just enough
              bleed to take the digital edge off.
            </p>
          </div>
          <div className="space-y-5">
            {sliderConfigs.map((slider) => (
              <label key={slider.key} className="block space-y-2">
                <div className="flex items-center justify-between gap-4">
                  <span className="text-sm font-medium">{slider.label}</span>
                  <span className="text-sm font-mono text-fd-muted-foreground">
                    {formatSliderValue(slider.key, settings[slider.key])}
                  </span>
                </div>
                <input
                  type="range"
                  min={slider.min}
                  max={slider.max}
                  step={slider.step}
                  value={settings[slider.key]}
                  onChange={(event) =>
                    setSettings((current) => ({
                      ...current,
                      [slider.key]: Number(event.target.value),
                    }))
                  }
                  className="w-full accent-fd-primary"
                />
                <p className="text-sm leading-relaxed text-fd-muted-foreground">
                  {slider.description}
                </p>
              </label>
            ))}
          </div>
        </aside>

        <div className="space-y-3">
          <div className="flex items-center justify-between gap-4">
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Preview
            </p>
            <div className="text-right">
              <p className="text-sm text-fd-muted-foreground">
                {previewMode === "original"
                  ? "Crisp source SVG"
                  : "Analogue wobble with pen-like edges"}
              </p>
              <p className="text-xs text-fd-muted-foreground">{fileName}</p>
            </div>
          </div>
          <div className="grid grid-cols-2 rounded-full border border-fd-border bg-fd-card/60 p-1">
            {(["original", "plotted"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => setPreviewMode(mode)}
                className={`rounded-full px-4 py-2.5 text-sm font-medium transition-colors ${
                  previewMode === mode
                    ? "bg-fd-background text-fd-foreground shadow-sm"
                    : "text-fd-muted-foreground hover:text-fd-foreground"
                }`}
                aria-pressed={previewMode === mode}
              >
                {mode === "original" ? "Original" : "Plotted"}
              </button>
            ))}
          </div>
          <div
            role="button"
            tabIndex={0}
            onClick={() => inputRef.current?.click()}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                inputRef.current?.click();
              }
            }}
            onDragEnter={() => setIsDragging(true)}
            onDragLeave={(event) => {
              if (event.currentTarget.contains(event.relatedTarget as Node | null)) return;
              setIsDragging(false);
            }}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDrop={(event) => {
              event.preventDefault();
              setIsDragging(false);

              const file = event.dataTransfer.files?.[0];
              if (file) void loadFile(file);
            }}
            aria-label="Drop an SVG onto the preview or click to choose a file"
            className={`relative aspect-[4/3] overflow-hidden rounded-[2rem] border bg-white p-[5%] shadow-[inset_0_0_0_1px_rgba(15,23,42,0.04)] transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-fd-primary/60 ${
              isDragging
                ? "border-fd-primary bg-fd-primary/4"
                : "border-fd-border hover:border-fd-primary/40"
            }`}
          >
            {previewMarkup ? (
              <div className="h-full w-full" dangerouslySetInnerHTML={{ __html: previewMarkup }} />
            ) : null}
            <div className="pointer-events-none absolute inset-x-[5%] bottom-[5%] flex justify-between gap-4">
              <div className="rounded-full border border-black/8 bg-white/88 px-3 py-1 text-xs font-medium text-fd-muted-foreground shadow-sm backdrop-blur-sm">
                {isDragging ? "Drop SVG to preview it" : "Drop SVG here or click to choose"}
              </div>
              <div className="rounded-full border border-black/8 bg-white/88 px-3 py-1 text-xs text-fd-muted-foreground shadow-sm backdrop-blur-sm">
                Sharp diagrams work best
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
