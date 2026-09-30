"use client";

import { useMemo, useRef, useState } from "react";
import { Button } from "@astryxdesign/core/Button";
import { Heading } from "@astryxdesign/core/Heading";
import { Text } from "@astryxdesign/core/Text";
import type { Flute, Layer, StipplePattern } from "./stipple";
import { drawStipple, StippleCanvas } from "./stipple-canvas";
import { gridPattern, stripePattern } from "./stipple-presets";

const presets: Record<string, StipplePattern> = { stripes: stripePattern, grid: gridPattern };

const sliders = [
  { key: "spacing", min: 0.002, max: 0.03, step: 0.0005 },
  { key: "radius", min: 0.0005, max: 0.015, step: 0.0005 },
  { key: "gain", min: 0.2, max: 4, step: 0.05 },
  { key: "jitter", min: 0, max: 1, step: 0.05 },
  { key: "seed", min: 1, max: 100, step: 1 },
] as const;

const backgrounds = { dark: "#000000", light: "#f5f5f4", none: "transparent" };

const flutesOf = (pattern: StipplePattern) =>
  pattern.layers.flatMap((layer) =>
    (layer.warps ?? []).filter((warp): warp is Flute => warp.type === "flute"),
  );

/**
 * Sets a flute setting on every layer at once, keeping each layer's sign for
 * scale and shift so mirrored inks stay mirrored and ribs stay aligned.
 */
function setFlutes(pattern: StipplePattern, key: keyof Flute, value: number): StipplePattern {
  const signed = key === "scale" || key === "shift";
  return mapFlutes(pattern, (flute) => {
    const sign = signed && ((flute[key] as number | undefined) ?? 1) < 0 ? -1 : 1;
    return { ...flute, [key]: sign * value };
  });
}

function mapFlutes(pattern: StipplePattern, update: (flute: Flute) => Flute): StipplePattern {
  return {
    ...pattern,
    layers: pattern.layers.map((layer) => ({
      ...layer,
      warps: layer.warps?.map((warp) => (warp.type === "flute" ? update(warp) : warp)),
    })),
  };
}

function setRadials(
  pattern: StipplePattern,
  key: "radius" | "gamma",
  value: number,
): StipplePattern {
  return {
    ...pattern,
    layers: pattern.layers.map((layer) =>
      layer.source.type === "radial"
        ? { ...layer, source: { ...layer.source, [key]: value } }
        : layer,
    ),
  };
}

type Projection = NonNullable<Flute["projection"]>;

const fluteSliders = [
  { key: "period", label: "Fluting width", min: 0.02, max: 0.5, step: 0.005 },
  { key: "angle", label: "Fluting angle", min: 0, max: 180, step: 1 },
  { key: "falloff", label: "Fluting falloff", min: 0.1, max: 6, step: 0.1, only: "orthographic" },
  { key: "scale", label: "Fluting strength", min: 0, max: 30, step: 0.25, only: "perspective" },
  { key: "shift", label: "Fluting offset", min: 0, max: 15, step: 0.25, only: "perspective" },
  { key: "bend", label: "Fluting bend", min: -1, max: 2, step: 0.05, only: "perspective" },
] as const satisfies {
  key: keyof Flute;
  label: string;
  min: number;
  max: number;
  step: number;
  only?: Projection;
}[];

const defaultFluteValue = { falloff: 1, scale: 1 } as Partial<Record<keyof Flute, number>>;

const radialSliders = [
  { key: "radius", label: "Gradient radius", min: 0.2, max: 4, step: 0.05 },
  { key: "gamma", label: "Gradient falloff", min: 0.2, max: 5, step: 0.05 },
] as const;

/** Draggable dot per radial gradient centre, in the canvas's pattern units. */
function CentreHandles({
  pattern,
  onMove,
}: {
  pattern: StipplePattern;
  onMove: (index: number, x: number, y: number) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const drag = (index: number) => (event: React.PointerEvent<HTMLButtonElement>) => {
    const box = ref.current?.getBoundingClientRect();
    if (!box) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    const move = (e: PointerEvent) => {
      const unit = box.height / 2;
      const x = (e.clientX - box.left - box.width / 2) / unit;
      const y = (e.clientY - box.top - box.height / 2) / unit;
      onMove(index, Math.round(x * 1000) / 1000, Math.round(y * 1000) / 1000);
    };
    const target = event.currentTarget;
    const up = () => {
      target.removeEventListener("pointermove", move);
      target.removeEventListener("pointerup", up);
    };
    target.addEventListener("pointermove", move);
    target.addEventListener("pointerup", up);
  };
  return (
    <div ref={ref} className="pointer-events-none absolute inset-0">
      {pattern.layers.map((layer: Layer, index) =>
        layer.source.type === "radial" ? (
          <button
            key={index}
            type="button"
            aria-label={`Move gradient centre ${index + 1}`}
            className="pointer-events-auto absolute size-5 -translate-x-1/2 -translate-y-1/2 cursor-grab rounded-full border-2 border-white shadow active:cursor-grabbing"
            style={{
              left: `calc(50% + ${layer.source.x ?? 0} * 50cqh)`,
              top: `calc(50% + ${layer.source.y ?? 0} * 50cqh)`,
              background: pattern.inks[layer.ink],
            }}
            onPointerDown={drag(index)}
          />
        ) : null,
      )}
    </div>
  );
}

/** Unlisted page for tuning stipple patterns and exporting them as images. */
export function StipplePlayground() {
  const [source, setSource] = useState(() => JSON.stringify(stripePattern, null, 2));
  const [aspect, setAspect] = useState(1.1);
  const [background, setBackground] = useState<keyof typeof backgrounds>("dark");
  const lastGood = useRef(stripePattern);
  const { pattern, error } = useMemo(() => {
    try {
      lastGood.current = JSON.parse(source) as StipplePattern;
      return { pattern: lastGood.current, error: null };
    } catch (error) {
      return { pattern: lastGood.current, error: String(error) };
    }
  }, [source]);

  const [showHandles, setShowHandles] = useState(true);
  const commit = (next: StipplePattern) => setSource(JSON.stringify(next, null, 2));
  const update = (key: string, value: number) => commit({ ...pattern, [key]: value });
  const moveCentre = (index: number, x: number, y: number) =>
    commit({
      ...pattern,
      layers: pattern.layers.map((layer, i) =>
        i === index && layer.source.type === "radial"
          ? { ...layer, source: { ...layer.source, x, y } }
          : layer,
      ),
    });
  const flute = flutesOf(pattern)[0];
  const projection: Projection = flute?.projection ?? "orthographic";
  const radial = pattern.layers.find((layer) => layer.source.type === "radial")?.source;

  const exportPng = () => {
    const canvas = document.createElement("canvas");
    canvas.height = 3000;
    canvas.width = Math.round(3000 * aspect);
    drawStipple(canvas, pattern);
    canvas.toBlob((blob) => {
      if (!blob) return;
      const link = document.createElement("a");
      link.href = URL.createObjectURL(blob);
      link.download = "stipple.png";
      link.click();
      URL.revokeObjectURL(link.href);
    });
  };

  return (
    <div className="home-container grid gap-8 py-12 lg:grid-cols-[minmax(0,1fr)_24rem]">
      <div className="space-y-4">
        <Heading level={1} type="display-3">
          Stipple patterns
        </Heading>
        <div
          className="relative overflow-hidden rounded-(--radius-container) border border-(--color-border)"
          style={{
            background: backgrounds[background],
            containerType: "size",
            aspectRatio: aspect,
          }}
        >
          <StippleCanvas pattern={pattern} className="block size-full" />
          {showHandles && <CentreHandles pattern={pattern} onMove={moveCentre} />}
        </div>
        <Text as="p" display="block" color="secondary">
          Drag the round handles to move each ink's gradient centre. One pattern unit is half the
          image height. Each layer reads a gradient through its warps (fluted glass, mirror,
          rotate), in order from the page towards the source. The fluting and gradient sliders set
          every layer at once. Exports are 3000 px tall with a transparent background.
        </Text>
      </div>
      <div className="space-y-4">
        <label className="block space-y-1">
          <Text weight="medium">Preset</Text>
          <select
            className="w-full rounded border border-(--color-border) bg-transparent p-2"
            onChange={(event) => setSource(JSON.stringify(presets[event.target.value], null, 2))}
          >
            {Object.keys(presets).map((name) => (
              <option key={name}>{name}</option>
            ))}
          </select>
        </label>
        {(
          [
            ["blend", ["screen", "max", "multiply"]],
            ["sampling", ["independent", "shared"]],
          ] as const
        ).map(([key, options]) => (
          <label key={key} className="block space-y-1">
            <Text weight="medium">{key}</Text>
            <select
              className="w-full rounded border border-(--color-border) bg-transparent p-2"
              value={pattern[key] ?? options[0]}
              onChange={(event) =>
                setSource(JSON.stringify({ ...pattern, [key]: event.target.value }, null, 2))
              }
            >
              {options.map((option) => (
                <option key={option}>{option}</option>
              ))}
            </select>
          </label>
        ))}
        {flute && (
          <label className="block space-y-1">
            <Text weight="medium">Fluting projection</Text>
            <select
              className="w-full rounded border border-(--color-border) bg-transparent p-2"
              value={projection}
              onChange={(event) =>
                commit(
                  mapFlutes(pattern, (f) => ({
                    ...f,
                    projection: event.target.value as Projection,
                  })),
                )
              }
            >
              <option>orthographic</option>
              <option>perspective</option>
            </select>
          </label>
        )}
        {flute && projection === "orthographic" && (
          <Button
            label="Swap sharp edges"
            variant="secondary"
            onClick={() => commit(mapFlutes(pattern, (f) => ({ ...f, mirror: !f.mirror })))}
          />
        )}
        {flute &&
          fluteSliders.map(({ key, label, min, max, step, ...rest }) =>
            "only" in rest && rest.only !== projection ? null : (
              <label key={key} className="block space-y-1">
                <Text weight="medium">
                  {label} {Math.abs(flute[key] ?? defaultFluteValue[key] ?? 0)}
                </Text>
                <input
                  type="range"
                  className="w-full"
                  min={min}
                  max={max}
                  step={step}
                  value={Math.abs(flute[key] ?? defaultFluteValue[key] ?? 0)}
                  onChange={(event) => commit(setFlutes(pattern, key, Number(event.target.value)))}
                />
              </label>
            ),
          )}
        {radial?.type === "radial" &&
          radialSliders.map(({ key, label, min, max, step }) => (
            <label key={key} className="block space-y-1">
              <Text weight="medium">
                {label} {radial[key] ?? 1}
              </Text>
              <input
                type="range"
                className="w-full"
                min={min}
                max={max}
                step={step}
                value={radial[key] ?? 1}
                onChange={(event) => commit(setRadials(pattern, key, Number(event.target.value)))}
              />
            </label>
          ))}
        {sliders.map(({ key, min, max, step }) => (
          <label key={key} className="block space-y-1">
            <Text weight="medium">
              {key} {pattern[key] ?? ""}
            </Text>
            <input
              type="range"
              className="w-full"
              min={min}
              max={max}
              step={step}
              value={
                pattern[key] ?? (key === "seed" ? 1 : key === "jitter" || key === "gain" ? 1 : 0)
              }
              onChange={(event) => update(key, Number(event.target.value))}
            />
          </label>
        ))}
        <label className="block space-y-1">
          <Text weight="medium">Aspect {aspect}</Text>
          <input
            type="range"
            className="w-full"
            min={0.4}
            max={2.5}
            step={0.05}
            value={aspect}
            onChange={(event) => setAspect(Number(event.target.value))}
          />
        </label>
        <div className="flex flex-wrap gap-2">
          {(Object.keys(backgrounds) as (keyof typeof backgrounds)[]).map((name) => (
            <Button
              key={name}
              label={`${name} background`}
              variant={background === name ? "primary" : "secondary"}
              onClick={() => setBackground(name)}
            />
          ))}
          <Button
            label={showHandles ? "Hide handles" : "Show handles"}
            variant="secondary"
            onClick={() => setShowHandles(!showHandles)}
          />
          <Button label="Export PNG" onClick={exportPng} />
          <Button
            label="Copy JSON"
            variant="secondary"
            onClick={() => navigator.clipboard.writeText(source)}
          />
        </div>
        <textarea
          className="h-[32rem] w-full rounded border border-(--color-border) bg-transparent p-2 font-mono text-xs"
          spellCheck={false}
          value={source}
          onChange={(event) => setSource(event.target.value)}
        />
        {error && (
          <Text as="p" display="block" color="secondary">
            {error}
          </Text>
        )}
      </div>
    </div>
  );
}
