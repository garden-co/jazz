import { describe, expect, it } from "vitest";
import {
  buildFilteredSvg,
  buildOriginalSvg,
  buildPauseTrailCircles,
  type PlotterSettings,
  type PreparedSvg,
} from "../../docs/components/playgrounds/svg-plotter-render";

const preparedSvg: PreparedSvg = {
  contentMarkup: '<path d="M0 0L100 0" stroke="#2e4ae3" stroke-width="3.28"/>',
  pauseDots: [
    {
      color: "#2e4ae3",
      cx: 100,
      cy: 0,
      lingerStrength: 1.2,
      strokeWidth: 3.28,
      trailX: -1,
      trailY: 0,
    },
  ],
  preserveAspectRatio: "xMidYMid meet",
  viewBox: "0 0 100 20",
};

const settings: PlotterSettings = {
  bleed: 0.22,
  endLinger: 0.18,
  frequency: 0.0155,
  opacity: 0.88,
  tipWidth: 0.02,
  wobble: 1,
};

describe("buildFilteredSvg", () => {
  it("can wrap the prepared svg without plotter filters for original view", () => {
    const svg = buildOriginalSvg(preparedSvg);

    expect(svg).toContain('viewBox="0 0 100 20"');
    expect(svg).toContain(preparedSvg.contentMarkup);
    expect(svg).not.toContain('filter id="plotter-pen"');
  });

  it("turns a linger point into a small trailing teardrop of circles", () => {
    const circles = buildPauseTrailCircles(preparedSvg.pauseDots[0]!, settings);

    expect(circles).toHaveLength(3);
    expect(circles[0]!.cx).toBeLessThan(circles[1]!.cx);
    expect(circles[1]!.cx).toBeLessThan(circles[2]!.cx);
    expect(circles[0]!.r).toBeLessThan(circles[1]!.r);
    expect(circles[1]!.r).toBeLessThan(circles[2]!.r);
    expect(circles[2]!.cx).toBeCloseTo(100, 2);
  });

  it("adds a fibrous bleed pass instead of only blurring the mark", () => {
    const svg = buildFilteredSvg(preparedSvg, settings, 11);

    expect(svg).toContain('result="bleed-noise"');
    expect(svg).toContain('result="bleed-fiber"');
    expect(svg).toContain('result="pass-a-edge"');
  });

  it("shapes ink density inside the filter instead of only using group opacity", () => {
    const svg = buildFilteredSvg(preparedSvg, settings, 11);

    expect(svg).toContain("<feComponentTransfer");
    expect(svg).toContain('<feFuncA type="gamma"');
    expect(svg).not.toContain('filter="url(#plotter-pen)" opacity=');
  });
});
