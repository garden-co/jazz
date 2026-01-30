import { createContext, useContext } from "react";

export type MediaStatus = "playing" | "paused" | "stopped";

export interface TrackMetadata {
  title: string;
  artist?: string;
  duration?: number;
}

type EventType =
  | "statusChange"
  | "timeUpdate"
  | "loaded"
  | "ended"
  | "stallChange"
  | "durationChange"
  | "error";

type EventCallback = () => void;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

type TrackHandler = () => void | Promise<void>;

export class AudioManager {
  private _mediaElement: HTMLAudioElement;
  private _audioObjectURL: string | null = null;
  private _status: MediaStatus = "stopped";
  private _stalled: boolean = false;
  private _listeners: Map<EventType, Set<EventCallback>> = new Map();
  private _eventCleanup: Array<() => void> = [];

  // Track navigation handlers
  private _nextTrackHandler: TrackHandler | null = null;
  private _previousTrackHandler: TrackHandler | null = null;

  // Auto-advance and keyboard options
  private _autoAdvance: boolean = true;
  private _keyboardShortcutsEnabled: boolean = false;

  constructor() {
    this._mediaElement = new Audio();
    this._setupEventListeners();
  }

  private _setupEventListeners() {
    const audio = this._mediaElement;

    const addListener = <K extends keyof HTMLMediaElementEventMap>(
      event: K,
      handler: (e: HTMLMediaElementEventMap[K]) => void,
    ) => {
      audio.addEventListener(event, handler);
      this._eventCleanup.push(() => audio.removeEventListener(event, handler));
    };

    addListener("play", () => {
      this._setStatus("playing");
    });

    addListener("pause", () => {
      if (!audio.ended) {
        this._setStatus("paused");
      }
    });

    addListener("ended", () => {
      this._setStatus("stopped");
      this._emit("ended");

      // Auto-advance to next track
      if (this._autoAdvance) {
        this.nextTrack();
      }
    });

    addListener("timeupdate", () => {
      this._emit("timeUpdate");
    });

    addListener("durationchange", () => {
      this._setStalled(false);
      this._emit("durationChange");
      this._emit("loaded");
    });

    addListener("waiting", () => {
      this._setStalled(true);
    });

    addListener("playing", () => {
      this._setStalled(false);
    });

    addListener("canplay", () => {
      this._setStalled(false);
    });

    addListener("error", () => {
      const error = audio.error;
      if (error) {
        console.error(`Audio error [${error.code}]: ${error.message}`);
      }
      this._emit("error");
      // Treat errors as end of track to avoid stuck state
      this._setStatus("stopped");
      this._emit("ended");
    });
  }

  // Event emitter methods
  on(event: EventType, callback: EventCallback): () => void {
    if (!this._listeners.has(event)) {
      this._listeners.set(event, new Set());
    }
    this._listeners.get(event)!.add(callback);

    // Return unsubscribe function
    return () => {
      this._listeners.get(event)?.delete(callback);
    };
  }

  private _emit(event: EventType) {
    this._listeners.get(event)?.forEach((cb) => cb());
  }

  private _setStatus(status: MediaStatus) {
    if (this._status !== status) {
      this._status = status;
      this._emit("statusChange");
      this._updateMediaSessionState();
    }
  }

  private _setStalled(stalled: boolean) {
    if (this._stalled !== stalled) {
      this._stalled = stalled;
      this._emit("stallChange");
    }
  }

  // Public getters
  get status(): MediaStatus {
    return this._status;
  }

  get isPlaying(): boolean {
    return this._status === "playing";
  }

  get isPaused(): boolean {
    return this._status === "paused";
  }

  get isStopped(): boolean {
    return this._status === "stopped";
  }

  get isStalled(): boolean {
    return this._stalled;
  }

  get duration(): number {
    const { duration } = this._mediaElement;
    // Handle NaN and Infinity (Safari iOS issue with missing Accept-Ranges header)
    return isNaN(duration) || !isFinite(duration) ? 0 : duration;
  }

  get currentTime(): number {
    return this._mediaElement.currentTime;
  }

  // Audio loading
  async unload() {
    if (this._audioObjectURL) {
      URL.revokeObjectURL(this._audioObjectURL);
      this._audioObjectURL = null;
    }
    this._mediaElement.src = "";
    this._setStatus("stopped");
    this._clearMediaSession();
  }

  async load(file: Blob) {
    await this.unload();

    const audioObjectURL = URL.createObjectURL(file);
    this._audioObjectURL = audioObjectURL;
    this._mediaElement.src = audioObjectURL;
  }

  // Playback controls
  async play() {
    if (this._status === "stopped" || this._mediaElement.ended) {
      this.seekTo(0);
    }

    try {
      await this._mediaElement.play();
    } catch (err) {
      // Play can fail if user hasn't interacted with the page yet
      console.warn("Playback failed:", err);
    }
  }

  pause() {
    this._mediaElement.pause();
  }

  stop() {
    this._mediaElement.pause();
    this._mediaElement.currentTime = 0;
    this._setStatus("stopped");
  }

  togglePlayPause = () => {
    if (this._status === "playing") {
      this.pause();
    } else {
      this.play();
    }
  };

  // Seeking
  seekTo(time: number) {
    const clampedTime = clamp(time, 0, this.duration || 0);
    this._mediaElement.currentTime = clampedTime;
    this._emit("timeUpdate");
  }

  seekBy(delta: number) {
    this.seekTo(this.currentTime + delta);
  }

  // Track navigation
  setNextTrackHandler(handler: TrackHandler | null) {
    this._nextTrackHandler = handler;
    this._updateMediaSessionHandlers();
  }

  setPreviousTrackHandler(handler: TrackHandler | null) {
    this._previousTrackHandler = handler;
    this._updateMediaSessionHandlers();
  }

  async nextTrack() {
    if (this._nextTrackHandler) {
      await this._nextTrackHandler();
    }
  }

  async previousTrack() {
    if (this._previousTrackHandler) {
      await this._previousTrackHandler();
    }
  }

  // MediaSession API for metadata
  setMetadata(metadata: TrackMetadata) {
    if (!("mediaSession" in navigator)) {
      return;
    }

    navigator.mediaSession.metadata = new MediaMetadata({
      title: metadata.title,
      artist: metadata.artist ?? "",
      album: "",
      artwork: [],
    });

    this._updateMediaSessionHandlers();
  }

  private _updateMediaSessionHandlers() {
    if (!("mediaSession" in navigator)) {
      return;
    }

    navigator.mediaSession.setActionHandler("play", () => this.play());
    navigator.mediaSession.setActionHandler("pause", () => this.pause());
    navigator.mediaSession.setActionHandler("stop", () => this.stop());
    navigator.mediaSession.setActionHandler("seekbackward", (details) => {
      this.seekBy(-(details.seekOffset ?? 10));
    });
    navigator.mediaSession.setActionHandler("seekforward", (details) => {
      this.seekBy(details.seekOffset ?? 10);
    });
    navigator.mediaSession.setActionHandler("seekto", (details) => {
      if (details.seekTime !== undefined) {
        this.seekTo(details.seekTime);
      }
    });

    // Next/previous track handlers (only enabled when handlers are set)
    navigator.mediaSession.setActionHandler(
      "nexttrack",
      this._nextTrackHandler ? () => this.nextTrack() : null,
    );
    navigator.mediaSession.setActionHandler(
      "previoustrack",
      this._previousTrackHandler ? () => this.previousTrack() : null,
    );
  }

  private _updateMediaSessionState() {
    if (!("mediaSession" in navigator)) {
      return;
    }

    switch (this._status) {
      case "playing":
        navigator.mediaSession.playbackState = "playing";
        break;
      case "paused":
        navigator.mediaSession.playbackState = "paused";
        break;
      case "stopped":
        navigator.mediaSession.playbackState = "none";
        break;
    }
  }

  private _clearMediaSession() {
    if (!("mediaSession" in navigator)) {
      return;
    }

    navigator.mediaSession.metadata = null;
    navigator.mediaSession.playbackState = "none";
  }

  // Keyboard shortcuts
  private _handleKeyboard = (evt: KeyboardEvent) => {
    // Only handle when body is focused (not in input fields)
    if (document.activeElement !== document.body) {
      return;
    }

    switch (evt.code) {
      case "Space":
        evt.preventDefault();
        this.togglePlayPause();
        break;
      case "ArrowLeft":
        evt.preventDefault();
        this.seekBy(-10);
        break;
      case "ArrowRight":
        evt.preventDefault();
        this.seekBy(10);
        break;
    }
  };

  enableKeyboardShortcuts() {
    if (this._keyboardShortcutsEnabled) return;

    window.addEventListener("keydown", this._handleKeyboard);
    this._keyboardShortcutsEnabled = true;
  }

  disableKeyboardShortcuts() {
    if (!this._keyboardShortcutsEnabled) return;

    window.removeEventListener("keydown", this._handleKeyboard);
    this._keyboardShortcutsEnabled = false;
  }

  // Auto-advance configuration
  setAutoAdvance(enabled: boolean) {
    this._autoAdvance = enabled;
  }

  get autoAdvance(): boolean {
    return this._autoAdvance;
  }

  // Cleanup
  dispose() {
    this.stop();
    this.unload();

    // Remove all event listeners
    this._eventCleanup.forEach((cleanup) => cleanup());
    this._eventCleanup = [];
    this._listeners.clear();
    this._clearMediaSession();
    this.disableKeyboardShortcuts();
  }
}

const context = createContext<AudioManager>(new AudioManager());

export function useAudioManager() {
  return useContext(context);
}

export const AudioManagerProvider = context.Provider;
