import { describe, expect, it, vi } from "vitest";
import { watchAutoplay, type ObserverFactory } from "./autoplay";

function setup(reducedMotion = false) {
  const listeners = new Map<string, () => void>();
  const video = {
    paused: true,
    play: vi.fn(async () => {
      video.paused = false;
    }),
    pause: vi.fn(() => {
      video.paused = true;
      listeners.get("pause")?.();
    }),
    addEventListener: (type: string, fn: () => void) => listeners.set(type, fn),
    removeEventListener: (type: string) => listeners.delete(type),
  };
  let callback: Parameters<ObserverFactory>[0] = () => {};
  const disconnect = vi.fn();
  const observe: ObserverFactory = (cb) => {
    callback = cb;
    return { observe: () => {}, disconnect };
  };
  const stop = watchAutoplay(video as never, { reducedMotion, observe });
  const view = (ratio: number) =>
    callback([{ isIntersecting: ratio > 0, intersectionRatio: ratio }]);
  const viewerPause = () => {
    video.paused = true;
    listeners.get("pause")?.();
  };
  return { video, view, stop, disconnect, viewerPause };
}

describe("watchAutoplay", () => {
  it("plays in view and pauses out of view", () => {
    const { video, view } = setup();
    view(0.8);
    expect(video.play).toHaveBeenCalledTimes(1);
    view(0.1);
    expect(video.pause).toHaveBeenCalledTimes(1);
    view(0.9);
    expect(video.play).toHaveBeenCalledTimes(2);
  });

  it("never starts with reduced motion", () => {
    const { video, view } = setup(true);
    view(1);
    expect(video.play).not.toHaveBeenCalled();
  });

  it("keeps a video the viewer paused paused until it leaves view", () => {
    const { video, view, viewerPause } = setup();
    view(1);
    viewerPause();
    view(0.9);
    expect(video.play).toHaveBeenCalledTimes(1);
    view(0);
    view(1);
    expect(video.play).toHaveBeenCalledTimes(2);
  });

  it("disconnects when stopped", () => {
    const { stop, disconnect } = setup();
    stop();
    expect(disconnect).toHaveBeenCalled();
  });
});
