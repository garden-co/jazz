import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { Metadata } from "next";
import { SvgPlotterPlayground } from "@/components/playgrounds/svg-plotter-playground";

export const metadata: Metadata = {
  title: "SVG Plotter Playground",
  description: "Give sharp SVG diagrams a slightly analogue, plotter-pen feel.",
};

export default async function PlotterPlaygroundPage() {
  let defaultSvg: string | null = null;

  for (const candidatePath of [
    join(process.cwd(), "public", "playgrounds", "plotter-default-preview.svg"),
    join(process.cwd(), "docs", "public", "playgrounds", "plotter-default-preview.svg"),
  ]) {
    try {
      defaultSvg = await readFile(candidatePath, "utf8");
      break;
    } catch {
      continue;
    }
  }

  if (!defaultSvg) {
    throw new Error("Could not load the default plotter preview SVG.");
  }

  return (
    <div className="w-full">
      <section className="w-full pb-24 pt-18 sm:pb-28 sm:pt-22 lg:pb-32 lg:pt-26">
        <div className="mx-auto w-full max-w-[min(96rem,100%-2rem)] px-4">
          <div className="max-w-[46rem] space-y-4">
            <p className="font-display text-xs font-semibold uppercase tracking-[0.18em] text-fd-muted-foreground">
              Playground
            </p>
            <h1 className="text-[clamp(3rem,8vw,5.8rem)] font-black leading-[0.9] tracking-[-0.05em]">
              SVGs that feel a little plotted
            </h1>
            <p className="max-w-[42rem] text-lg leading-relaxed text-fd-muted-foreground sm:text-xl">
              A tiny filter lab for taking crisp hero diagrams and giving them a machine-drawn,
              felt-tip touch. Drop in an SVG, tune the wobble, and see what feels right.
            </p>
          </div>

          <div className="mt-12">
            <SvgPlotterPlayground defaultFileName="Asset 21.svg" defaultSvg={defaultSvg} />
          </div>
        </div>
      </section>
    </div>
  );
}
