import { describe, expect, it } from "vitest";
import { isAudible, volumeToGain } from "./audio";
import { starterStep } from "./instruments";
import { strongestRole } from "./roles";
import { stepId } from "./step-id";
import {
  absoluteStepAt,
  retime,
  startPlayback,
  stepDurationMs,
  stepStartMs,
  stopPlayback,
  wrapStep,
  type TransportState,
} from "./transport";

const playing: TransportState = {
  playing: true,
  anchorStep: 0,
  anchorAt: 10_000,
  tempo: 120,
  patternId: "pattern-a",
};

describe("shared transport", () => {
  it("counts sixteenth notes from the anchor", () => {
    expect(stepDurationMs(120)).toBe(125);
    expect(absoluteStepAt(playing, 10_000)).toBe(0);
    expect(absoluteStepAt(playing, 10_124)).toBe(0);
    expect(absoluteStepAt(playing, 10_125)).toBe(1);
    expect(stepStartMs(playing, 8)).toBe(11_000);
  });

  it("wraps the playhead into the pattern", () => {
    expect(wrapStep(17, 16)).toBe(1);
    expect(wrapStep(-1, 16)).toBe(15);
  });

  it("holds the anchor step while stopped", () => {
    expect(absoluteStepAt({ ...playing, playing: false, anchorStep: 3 }, 99_999)).toBe(3);
  });

  it("starts and stops from step one with the current tempo and pattern", () => {
    expect(startPlayback(playing, 5)).toEqual({
      playing: true,
      bar: 0,
      observed_at: new Date(5),
      tempo_bpm: 120,
      pattern_id: "pattern-a",
    });
    expect(stopPlayback(playing, 6).playing).toBe(false);
  });

  it("changes tempo without jumping and never anchors in the future", () => {
    const now = 10_000 + 125 * 5 + 60;
    const next = retime(playing, now, { tempo: 90 });
    expect(next).toMatchObject({ playing: true, bar: 5, tempo_bpm: 90 });
    expect(next.observed_at.getTime()).toBe(10_625);
    expect(next.observed_at.getTime()).toBeLessThanOrEqual(now);
  });

  it("switches pattern while stopped without starting playback", () => {
    const next = retime({ ...playing, playing: false }, 1, { patternId: "pattern-b" });
    expect(next).toMatchObject({ playing: false, pattern_id: "pattern-b" });
  });
});

describe("mix and roles", () => {
  it("lets solo override mute on other tracks", () => {
    expect(isAudible({ muted: false, solo: false }, false)).toBe(true);
    expect(isAudible({ muted: true, solo: false }, false)).toBe(false);
    expect(isAudible({ muted: false, solo: false }, true)).toBe(false);
    expect(isAudible({ muted: false, solo: true }, true)).toBe(true);
  });

  it("maps volume onto a clamped gain curve", () => {
    expect(volumeToGain(0)).toBe(0);
    expect(volumeToGain(50)).toBe(0.25);
    expect(volumeToGain(150)).toBe(1);
  });

  it("uses the strongest of several membership rows", () => {
    expect(strongestRole(["viewer", "editor"])).toBe("editor");
    expect(strongestRole([])).toBeUndefined();
  });

  it("seeds a four-on-the-floor starter groove", () => {
    expect([0, 4, 8, 12].every((step) => starterStep("kick", step))).toBe(true);
    expect(starterStep("kick", 1)).toBe(false);
  });

  it("derives one uuid-shaped row id per pad", async () => {
    const id = await stepId("track", "pattern", 3);
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-a[0-9a-f]{3}-[0-9a-f]{12}$/);
    expect(await stepId("track", "pattern", 3)).toBe(id);
    expect(await stepId("track", "pattern", 4)).not.toBe(id);
  });
});
