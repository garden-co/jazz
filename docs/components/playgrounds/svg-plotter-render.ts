export type PlotterSettings = {
  bleed: number;
  endLinger: number;
  frequency: number;
  opacity: number;
  tipWidth: number;
  wobble: number;
};

export type PauseDot = {
  color: string;
  cx: number;
  cy: number;
  lingerStrength: number;
  strokeWidth: number;
  trailX: number;
  trailY: number;
};

export type PauseTrailCircle = {
  cx: number;
  cy: number;
  opacity: number;
  r: number;
};

export type PreparedSvg = {
  contentMarkup: string;
  pauseDots: PauseDot[];
  preserveAspectRatio: string;
  viewBox: string;
};

export function buildOriginalSvg(prepared: PreparedSvg) {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${prepared.viewBox}" width="100%" height="100%" preserveAspectRatio="${prepared.preserveAspectRatio}" fill="none">
    ${prepared.contentMarkup}
  </svg>`;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

export function buildPauseTrailCircles(
  dot: PauseDot,
  settings: PlotterSettings,
): PauseTrailCircle[] {
  const baseRadius = Math.max(
    0,
    dot.strokeWidth * settings.endLinger * (3.4 + dot.lingerStrength * 1.4),
  );
  const directionLength = Math.hypot(dot.trailX, dot.trailY);
  const normalizedTrail =
    directionLength > 0.001
      ? {
          x: dot.trailX / directionLength,
          y: dot.trailY / directionLength,
        }
      : { x: 0, y: 0 };
  const trailLength = Math.max(dot.strokeWidth * 0.55, baseRadius * 0.82);
  const alphaBase = clamp(0.76 + settings.endLinger * 0.42, 0, 1);
  const steps = [
    { offset: 0.92, opacity: 0.42, radiusScale: 0.54 },
    { offset: 0.48, opacity: 0.66, radiusScale: 0.78 },
    { offset: 0, opacity: 1, radiusScale: 1 },
  ];

  return steps.map((step) => ({
    cx: dot.cx + normalizedTrail.x * trailLength * step.offset,
    cy: dot.cy + normalizedTrail.y * trailLength * step.offset,
    opacity: clamp(alphaBase * step.opacity, 0, 1),
    r: baseRadius * step.radiusScale,
  }));
}

export function buildFilteredSvg(prepared: PreparedSvg, settings: PlotterSettings, seed: number) {
  const densityProgress = Math.max(0, Math.min(1, (settings.opacity - 0.55) / 0.45));
  const alphaAmplitude = (0.68 + densityProgress * 0.52).toFixed(2);
  const alphaExponent = (1.45 - densityProgress * 0.7).toFixed(2);
  const alphaOffset = (-0.04 + densityProgress * 0.06).toFixed(2);
  const bleedFrequency = (settings.frequency * 26).toFixed(4);
  const bleedFiberScale = Math.max(0.08, settings.bleed * 0.9).toFixed(2);
  const edgeRecoverBlur = Math.max(0.01, settings.bleed * 0.2).toFixed(2);
  const edgeRecover = (0.18 + settings.bleed * 0.55).toFixed(2);
  const grainFrequency = (settings.frequency * 12).toFixed(4);
  const grainScale = Math.max(0, settings.wobble * 0.12).toFixed(2);
  const pauseMarkup =
    settings.endLinger > 0
      ? prepared.pauseDots
          .flatMap((dot) =>
            buildPauseTrailCircles(dot, settings).map(
              (circle) =>
                `<circle cx="${circle.cx.toFixed(2)}" cy="${circle.cy.toFixed(2)}" r="${circle.r.toFixed(2)}" fill="${dot.color}" opacity="${circle.opacity.toFixed(2)}" />`,
            ),
          )
          .join("")
      : "";

  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${prepared.viewBox}" width="100%" height="100%" preserveAspectRatio="${prepared.preserveAspectRatio}" fill="none">
    <defs>
      <filter id="plotter-pen" x="-6%" y="-6%" width="112%" height="112%" color-interpolation-filters="sRGB">
        <feTurbulence type="fractalNoise" baseFrequency="${settings.frequency.toFixed(4)}" numOctaves="2" seed="${seed}" result="noise-a"/>
        <feDisplacementMap in="SourceGraphic" in2="noise-a" scale="${settings.wobble.toFixed(2)}" xChannelSelector="R" yChannelSelector="G" result="pass-a-raw"/>
        <feMorphology in="pass-a-raw" operator="dilate" radius="${settings.tipWidth.toFixed(2)}" result="pass-a-tip"/>
        <feGaussianBlur in="pass-a-tip" stdDeviation="${settings.bleed.toFixed(2)}" result="pass-a-soft"/>

        <feTurbulence type="fractalNoise" baseFrequency="${bleedFrequency}" numOctaves="2" seed="${
          seed + 17
        }" result="bleed-noise"/>
        <feDisplacementMap in="pass-a-soft" in2="bleed-noise" scale="${bleedFiberScale}" xChannelSelector="R" yChannelSelector="G" result="bleed-fiber"/>
        <feGaussianBlur in="pass-a-tip" stdDeviation="${edgeRecoverBlur}" result="pass-a-edge-source"/>
        <feComposite in="bleed-fiber" in2="pass-a-edge-source" operator="arithmetic" k1="0" k2="0.96" k3="${edgeRecover}" k4="0" result="pass-a-edge"/>

        <feTurbulence type="fractalNoise" baseFrequency="${grainFrequency}" numOctaves="1" seed="${
          seed + 33
        }" result="grain-noise"/>
        <feDisplacementMap in="pass-a-edge" in2="grain-noise" scale="${grainScale}" result="grainy"/>
        <feComponentTransfer in="grainy" result="ink-density">
          <feFuncA type="gamma" amplitude="${alphaAmplitude}" exponent="${alphaExponent}" offset="${alphaOffset}"/>
        </feComponentTransfer>
      </filter>
    </defs>
    <g filter="url(#plotter-pen)">
      ${prepared.contentMarkup}
      ${pauseMarkup}
    </g>
  </svg>`;
}
