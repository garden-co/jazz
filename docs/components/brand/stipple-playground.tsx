"use client";

import { useMemo, useRef, useState } from "react";
import { Button } from "@astryxdesign/core/Button";
import { Heading } from "@astryxdesign/core/Heading";
import { Text } from "@astryxdesign/core/Text";
import type { StipplePattern } from "./stipple";
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

  const update = (key: string, value: number) =>
    setSource(JSON.stringify({ ...pattern, [key]: value }, null, 2));

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
          className="overflow-hidden rounded-(--radius-container) border border-(--color-border)"
          style={{ background: backgrounds[background] }}
        >
          <StippleCanvas
            pattern={pattern}
            className="block w-full"
            style={{ aspectRatio: aspect }}
          />
        </div>
        <Text as="p" display="block" color="secondary">
          One pattern unit is half the image height. Each layer reads a gradient through its warps
          (fluted glass, mirror, rotate), in order from the page towards the source. Exports are
          3000 px tall with a transparent background.
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
