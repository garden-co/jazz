import { getContext, setContext } from "svelte";

const AUDIO_CONTEXT_KEY = Symbol("wequencer-audio");

export interface WequencerAudio {
  play: () => Promise<void>;
  stop: () => void;
  startContext: () => Promise<void>;
  easterEgg: () => void;
  readonly uiBeat: number;
  readonly isPlaying: boolean;
  readonly isContextActive: boolean;
  readonly countdownMs: number;
  readonly bpm: number;
  setBpm: (bpm: number) => void;
  readonly syncAlignment: boolean;
  setSyncAlignment: (enabled: boolean) => void;
  readonly beatCount: number;
  setBeatCount: (count: number) => void;
  readonly masterVolume: number;
  setMasterVolume: (db: number) => void;
  getInstrumentVolume: (instrumentId: string) => number;
  setInstrumentVolume: (instrumentId: string, db: number) => void;
  getInstrumentPan: (instrumentId: string) => number;
  setInstrumentPan: (instrumentId: string, pan: number) => void;
}

export function getAudioContext(): WequencerAudio {
  const context = getContext<WequencerAudio>(AUDIO_CONTEXT_KEY);
  if (!context) {
    throw new Error("Audio context not found. Make sure AudioProvider is mounted.");
  }
  return context;
}

export function setAudioContext(context: WequencerAudio): void {
  setContext(AUDIO_CONTEXT_KEY, context);
}
