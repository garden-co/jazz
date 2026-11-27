"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { DrumSynth, PlaybackScheduler, type PlaybackSnapshot } from "@/lib/audio";
import { absoluteStepAt, wrapStep, type TransportState } from "@/lib/transport";
import type { Instrument } from "@/schema";

/**
 * Local sound for this device. Browsers only start audio after a gesture, so
 * sound is opt-in per bandmate; the shared transport decides what plays.
 */
export function useSequencerAudio(snapshot: () => PlaybackSnapshot) {
  const [isSoundOn, setIsSoundOn] = useState(false);
  const synthRef = useRef<DrumSynth | null>(null);
  const schedulerRef = useRef<PlaybackScheduler | null>(null);
  const snapshotRef = useRef(snapshot);
  snapshotRef.current = snapshot;

  const ensureSynth = useCallback(() => {
    if (!synthRef.current) {
      synthRef.current = new DrumSynth();
      schedulerRef.current = new PlaybackScheduler(synthRef.current, () => snapshotRef.current());
    }
    void synthRef.current.resume();
    return synthRef.current;
  }, []);

  const setSound = useCallback(
    (on: boolean) => {
      if (on) {
        ensureSynth();
        schedulerRef.current!.start();
      } else {
        schedulerRef.current?.stop();
      }
      setIsSoundOn(on);
    },
    [ensureSynth],
  );

  /** Plays one hit right away, for auditioning a track's instrument. */
  const preview = useCallback(
    (instrument: Instrument, level: number) => {
      const synth = ensureSynth();
      synth.trigger(instrument, synth.context.currentTime + 0.01, level, 0);
    },
    [ensureSynth],
  );

  useEffect(
    () => () => {
      schedulerRef.current?.stop();
      void synthRef.current?.close();
    },
    [],
  );

  return { isSoundOn, setSound, preview };
}

/** The step under the playhead on this client's clock, or null when stopped. */
export function usePlayhead(transport: TransportState, length: number) {
  const [step, setStep] = useState<number | null>(null);
  useEffect(() => {
    if (!transport.playing || length <= 0) {
      setStep(null);
      return;
    }
    let frame = 0;
    const update = () => {
      setStep(wrapStep(absoluteStepAt(transport, Date.now()), length));
      frame = requestAnimationFrame(update);
    };
    update();
    return () => cancelAnimationFrame(frame);
  }, [transport, length]);
  return step;
}
