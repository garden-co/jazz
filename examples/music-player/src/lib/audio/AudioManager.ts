import { createContext, useContext } from "react";
import {
  Input,
  ALL_FORMATS,
  AudioBufferSink,
  type WrappedAudioBuffer,
  type InputAudioTrack,
  BlobSource,
} from "mediabunny";
import { FileStreamSource } from "./FileStreamSource";

type PlayState = "playing" | "paused" | "stopped";

type AudioManagerEventMap = {
  play: Event;
  pause: Event;
  ended: Event;
  timeupdate: CustomEvent<number>;
};

export class AudioManager extends EventTarget {
  private audioContext: AudioContext | null = null;
  private gainNode: GainNode | null = null;

  private input: Input<FileStreamSource | BlobSource> | null = null;
  private audioTrack: InputAudioTrack | null = null;
  private audioSink: AudioBufferSink | null = null;

  /** The value of audioContext.currentTime when playback started */
  private audioContextStartTime: number | null = null;
  /** The timestamp within the media file when playback was started */
  private playbackTimeAtStart = 0;

  private audioBufferIterator: AsyncGenerator<
    WrappedAudioBuffer,
    void,
    unknown
  > | null = null;
  private queuedAudioNodes: Set<AudioBufferSourceNode> = new Set();
  private iteratorAborted = false;
  /** The next scheduled start time for buffer playback */
  private nextBufferStartTime = 0;

  private playState: PlayState = "stopped";
  private totalDuration = 0;
  private timeUpdateInterval: ReturnType<typeof setInterval> | null = null;

  on<K extends keyof AudioManagerEventMap>(
    event: K,
    callback: (e: AudioManagerEventMap[K]) => void,
  ): () => void {
    this.addEventListener(event, callback as EventListener);
    return () => this.removeEventListener(event, callback as EventListener);
  }

  private emit<K extends keyof AudioManagerEventMap>(
    event: K,
    detail?: K extends "timeupdate" ? number : never,
  ): void {
    if (event === "timeupdate") {
      this.dispatchEvent(new CustomEvent(event, { detail }));
    } else {
      this.dispatchEvent(new Event(event));
    }
  }

  private clearQueuedNodes(): void {
    for (const node of this.queuedAudioNodes) {
      try {
        node.stop();
        node.disconnect();
      } catch {
        // Node may already be stopped
      }
    }
    this.queuedAudioNodes.clear();
  }

  private async abortIterator(): Promise<void> {
    this.iteratorAborted = true;
    if (this.audioBufferIterator) {
      await this.audioBufferIterator.return();
      this.audioBufferIterator = null;
    }
  }

  async unloadCurrentAudio(): Promise<void> {
    this.stopTimeUpdates();
    this.clearQueuedNodes();
    await this.abortIterator();

    if (this.input) {
      this.input[Symbol.dispose]();
      this.input = null;
    }

    this.audioTrack = null;
    this.audioSink = null;
    this.playState = "stopped";
    this.audioContextStartTime = null;
    this.playbackTimeAtStart = 0;
    this.totalDuration = 0;
  }

  async loadAudio(source: FileStreamSource | BlobSource): Promise<void> {
    await this.unloadCurrentAudio();
    this.iteratorAborted = false;

    this.input = new Input({ source, formats: ALL_FORMATS });

    this.audioTrack = await this.input.getPrimaryAudioTrack();
    if (!this.audioTrack) {
      throw new Error("No audio track found in the media file");
    }

    if (!(await this.audioTrack.canDecode())) {
      throw new Error("Unable to decode the audio track");
    }

    // Create AudioContext with matching sample rate for correct playback
    if (this.audioContext) {
      await this.audioContext.close();
      this.audioContext = null;
      this.gainNode = null;
    }

    this.audioContext = new AudioContext({
      sampleRate: this.audioTrack.sampleRate,
    });
    this.gainNode = this.audioContext.createGain();
    this.gainNode.connect(this.audioContext.destination);

    this.totalDuration = await this.input.computeDuration();
    this.audioSink = new AudioBufferSink(this.audioTrack);
    this.playState = "stopped";
    this.playbackTimeAtStart = 0;
  }

  async play(): Promise<void> {
    if (!this.audioSink || !this.audioContext || !this.gainNode) {
      return;
    }

    if (this.audioContext.state === "suspended") {
      await this.audioContext.resume();
    }

    if (this.playState === "playing") {
      return;
    }

    if (this.playState === "stopped" || this.queuedAudioNodes.size === 0) {
      await this.startPlaybackFromTime(this.playbackTimeAtStart);
    } else {
      this.audioContextStartTime = this.audioContext.currentTime;
    }

    this.playState = "playing";
    this.startTimeUpdates();
    this.emit("play");
  }

  private async startPlaybackFromTime(startTime: number): Promise<void> {
    if (!this.audioSink || !this.audioContext || !this.gainNode) {
      return;
    }

    this.clearQueuedNodes();
    await this.abortIterator();

    this.iteratorAborted = false;
    this.playbackTimeAtStart = startTime;
    this.audioContextStartTime = this.audioContext.currentTime;
    this.nextBufferStartTime = this.audioContext.currentTime;

    this.audioBufferIterator = this.audioSink.buffers(startTime);
    this.pumpAudioBuffers();
  }

  private async pumpAudioBuffers(): Promise<void> {
    if (
      !this.audioBufferIterator ||
      !this.audioContext ||
      !this.gainNode ||
      this.iteratorAborted
    ) {
      return;
    }

    const { audioContext, gainNode } = this;

    try {
      for await (const { buffer } of this.audioBufferIterator) {
        if (this.iteratorAborted || this.playState !== "playing") {
          break;
        }

        const sourceNode = audioContext.createBufferSource();
        sourceNode.buffer = buffer;
        sourceNode.connect(gainNode);
        this.queuedAudioNodes.add(sourceNode);

        sourceNode.onended = () => {
          this.queuedAudioNodes.delete(sourceNode);
          sourceNode.disconnect();

          if (
            this.getCurrentTime() >= this.totalDuration &&
            this.playState === "playing"
          ) {
            this.playState = "stopped";
            this.playbackTimeAtStart = 0;
            this.stopTimeUpdates();
            this.emit("ended");
          }
        };

        const scheduleTime = Math.max(
          this.nextBufferStartTime,
          audioContext.currentTime,
        );
        sourceNode.start(scheduleTime);
        this.nextBufferStartTime = scheduleTime + buffer.duration;

        // Backpressure: wait if too many buffers are queued
        while (
          this.queuedAudioNodes.size > 3 &&
          this.playState === "playing" &&
          !this.iteratorAborted
        ) {
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }
    } catch (error) {
      if (!this.iteratorAborted) {
        console.error("Error pumping audio buffers:", error);
      }
    }
  }

  pause(): void {
    if (this.playState !== "playing") {
      return;
    }

    this.playbackTimeAtStart = this.getCurrentTime();
    this.clearQueuedNodes();
    this.iteratorAborted = true;

    this.playState = "paused";
    this.stopTimeUpdates();
    this.emit("pause");
  }

  async seek(time: number): Promise<void> {
    const wasPlaying = this.playState === "playing";
    time = Math.max(0, Math.min(time, this.totalDuration));

    if (wasPlaying) {
      this.pause();
    }

    this.playbackTimeAtStart = time;

    if (wasPlaying) {
      await this.play();
    } else {
      this.emit("timeupdate", time);
    }
  }

  getCurrentTime(): number {
    if (this.playState === "playing" && this.audioContextStartTime !== null) {
      const elapsed =
        this.audioContext!.currentTime - this.audioContextStartTime;
      return Math.min(this.playbackTimeAtStart + elapsed, this.totalDuration);
    }
    return this.playbackTimeAtStart;
  }

  getDuration(): number {
    return this.totalDuration;
  }

  isPlaying(): boolean {
    return this.playState === "playing";
  }

  private startTimeUpdates(): void {
    this.stopTimeUpdates();
    this.timeUpdateInterval = setInterval(() => {
      if (this.playState === "playing") {
        this.emit("timeupdate", this.getCurrentTime());
      }
    }, 50);
  }

  private stopTimeUpdates(): void {
    if (this.timeUpdateInterval) {
      clearInterval(this.timeUpdateInterval);
      this.timeUpdateInterval = null;
    }
  }

  destroy(): void {
    this.unloadCurrentAudio();
    if (this.audioContext) {
      this.audioContext.close();
      this.audioContext = null;
      this.gainNode = null;
    }
  }
}

const context = createContext<AudioManager>(new AudioManager());

export function useAudioManager() {
  return useContext(context);
}

export const AudioManagerProvider = context.Provider;
