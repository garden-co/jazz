export type ClockOffsetOptions = {
  windowSize?: number;
  outlierThresholdMs?: number;
  maxAbsOffsetMs?: number;
};

export type ClockOffsetSample = {
  serverTime: number;
  localReceiveTime: number;
};

const DEFAULT_WINDOW_SIZE = 32;
const DEFAULT_OUTLIER_THRESHOLD_MS = 2000;
const DEFAULT_MAX_ABS_OFFSET_MS = 24 * 60 * 60 * 1000;

export class ClockOffset {
  private readonly windowSize: number;
  private readonly outlierThresholdMs: number;
  private readonly maxAbsOffsetMs: number;
  private readonly samples: number[] = [];
  private cachedOffset = 0;

  constructor(options: ClockOffsetOptions = {}) {
    this.windowSize = Math.max(1, options.windowSize ?? DEFAULT_WINDOW_SIZE);
    this.outlierThresholdMs =
      options.outlierThresholdMs ?? DEFAULT_OUTLIER_THRESHOLD_MS;
    this.maxAbsOffsetMs = options.maxAbsOffsetMs ?? DEFAULT_MAX_ABS_OFFSET_MS;
  }

  addSample(sample: ClockOffsetSample): void {
    if (
      !Number.isFinite(sample.serverTime) ||
      !Number.isFinite(sample.localReceiveTime)
    ) {
      return;
    }

    const impliedOffset = sample.serverTime - sample.localReceiveTime;

    if (Math.abs(impliedOffset) > this.maxAbsOffsetMs) {
      return;
    }

    // Outlier gate: reject samples that are too far from the current estimate.
    // This prevents a single bad ping from poisoning the median.
    // Note: on the first sample this gate is skipped — only the magnitude cap applies.
    // A transient delay on that first ping can seed a poor baseline, causing subsequent
    // legitimate samples to be rejected. Worth revisiting once we have real-world data.
    if (
      this.samples.length > 0 &&
      Math.abs(impliedOffset - this.cachedOffset) > this.outlierThresholdMs
    ) {
      return;
    }

    this.samples.push(impliedOffset);
    if (this.samples.length > this.windowSize) {
      this.samples.shift();
    }

    this.cachedOffset = median(this.samples);
  }

  currentOffset(): number {
    return this.cachedOffset;
  }

  sampleCount(): number {
    return this.samples.length;
  }
}

function median(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  if (sorted.length % 2 === 0) {
    return (sorted[mid - 1]! + sorted[mid]!) / 2;
  }
  return sorted[mid]!;
}
