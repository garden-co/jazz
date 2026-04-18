export type PlotPoint = {
  x: number;
  y: number;
};

export type PausePoint = PlotPoint & {
  lingerStrength: number;
  trailX: number;
  trailY: number;
};

type SharpTurnOptions = {
  closed?: boolean;
  minSegmentLength?: number;
  minSpacing?: number;
  minTurnAngleDegrees?: number;
};

function distance(a: PlotPoint, b: PlotPoint) {
  return Math.hypot(a.x - b.x, a.y - b.y);
}

function toRadians(degrees: number) {
  return (degrees * Math.PI) / 180;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
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

export function collectSharpTurnPausePoints(
  points: PlotPoint[],
  options: SharpTurnOptions = {},
): PausePoint[] {
  if (points.length < 3) return [];

  const closed = options.closed ?? false;
  const minSegmentLength = options.minSegmentLength ?? 1;
  const minSpacing = options.minSpacing ?? 4;
  const minTurnAngle = toRadians(options.minTurnAngleDegrees ?? 28);
  const normalizedPoints =
    closed && distance(points[0]!, points.at(-1)!) < 0.001 ? points.slice(0, -1) : points;

  if (normalizedPoints.length < 3) return [];

  const pausePoints: PausePoint[] = [];

  const startIndex = closed ? 0 : 1;
  const endIndex = closed ? normalizedPoints.length : normalizedPoints.length - 1;

  for (let index = startIndex; index < endIndex; index += 1) {
    const previous = closed
      ? normalizedPoints[(index - 1 + normalizedPoints.length) % normalizedPoints.length]!
      : normalizedPoints[index - 1]!;
    const current = normalizedPoints[index]!;
    const next = closed
      ? normalizedPoints[(index + 1) % normalizedPoints.length]!
      : normalizedPoints[index + 1]!;

    const incoming = {
      x: current.x - previous.x,
      y: current.y - previous.y,
    };
    const outgoing = {
      x: next.x - current.x,
      y: next.y - current.y,
    };

    const incomingLength = Math.hypot(incoming.x, incoming.y);
    const outgoingLength = Math.hypot(outgoing.x, outgoing.y);

    if (incomingLength < minSegmentLength || outgoingLength < minSegmentLength) {
      continue;
    }

    const cosine =
      (incoming.x * outgoing.x + incoming.y * outgoing.y) / (incomingLength * outgoingLength);
    const turnAngle = Math.acos(clamp(cosine, -1, 1));

    if (!Number.isFinite(turnAngle) || turnAngle < minTurnAngle) {
      continue;
    }

    const lingerStrength = clamp(
      0.95 + ((turnAngle - minTurnAngle) / (Math.PI - minTurnAngle)) * 0.85,
      0.95,
      1.8,
    );
    const incomingUnit = normalizeVector(incoming.x, incoming.y);
    const outgoingUnit = normalizeVector(outgoing.x, outgoing.y);
    let trail = normalizeVector(
      -(incomingUnit.x + outgoingUnit.x),
      -(incomingUnit.y + outgoingUnit.y),
    );

    if (trail.x === 0 && trail.y === 0) {
      trail = normalizeVector(-outgoingUnit.x, -outgoingUnit.y);
    }

    const existingIndex = pausePoints.findIndex(
      (pausePoint) => distance(pausePoint, current) < minSpacing,
    );

    if (existingIndex === -1) {
      pausePoints.push({
        x: current.x,
        y: current.y,
        lingerStrength,
        trailX: trail.x,
        trailY: trail.y,
      });
      continue;
    }

    if (pausePoints[existingIndex]!.lingerStrength < lingerStrength) {
      pausePoints[existingIndex] = {
        x: current.x,
        y: current.y,
        lingerStrength,
        trailX: trail.x,
        trailY: trail.y,
      };
    }
  }

  return pausePoints;
}
