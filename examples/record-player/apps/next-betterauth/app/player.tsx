"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Pause, Play, SkipBack, SkipForward, Volume2, VolumeX } from "lucide-react";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { Slider } from "@astryxdesign/core/Slider";
import { Text } from "@astryxdesign/core/Text";
import { openTrackAudio, type OpenedAudio, type PlayableTrack } from "../src/audio-stream";
import type { JazzRecordPlayerStore } from "../src/record-player";
import { CoverArt } from "./cover-art";
import { formatDuration } from "./format";

type PlayerState = {
  queue: PlayableTrack[];
  index: number;
  current: PlayableTrack | undefined;
  isPlaying: boolean;
  /** Fraction of the current track's bytes read from Jazz so far. */
  loaded: number;
  error: string | undefined;
  playQueue(tracks: PlayableTrack[], startAt?: number): void;
  toggle(): void;
  next(): void;
  previous(): void;
};

const PlayerContext = createContext<PlayerState | null>(null);

export function usePlayer(): PlayerState {
  const player = useContext(PlayerContext);
  if (!player) throw new Error("usePlayer must be used inside <PlayerProvider>.");
  return player;
}

/** Owns the single `<audio>` element and the queue that feeds it. */
export function PlayerProvider({
  store,
  children,
}: {
  store: JazzRecordPlayerStore;
  children: ReactNode;
}) {
  const audioRef = useRef<HTMLAudioElement | null>(null);
  const [queue, setQueue] = useState<PlayableTrack[]>([]);
  const [index, setIndex] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [loaded, setLoaded] = useState(0);
  const [error, setError] = useState<string>();
  const [position, setPosition] = useState(0);
  const [duration, setDuration] = useState(0);
  const [volume, setVolume] = useState(0.8);
  const [muted, setMuted] = useState(false);
  const current = queue[index];

  // Open the current track from Jazz whenever it changes.
  useEffect(() => {
    const audio = audioRef.current;
    if (!audio || !current) return;
    const abort = new AbortController();
    let opened: OpenedAudio | undefined;
    setLoaded(0);
    setError(undefined);
    setPosition(0);
    setDuration(current.durationMs / 1000);
    void openTrackAudio(store, current, {
      signal: abort.signal,
      onProgress: (bytes, total) => setLoaded(total > 0 ? bytes / total : 1),
    })
      .then((result) => {
        if (abort.signal.aborted) return result.dispose();
        opened = result;
        audio.src = result.url;
        result.loaded.catch((cause: unknown) => {
          if (!abort.signal.aborted) setError(messageOf(cause));
        });
        return audio.play();
      })
      .catch((cause: unknown) => {
        if (!abort.signal.aborted) setError(messageOf(cause));
      });
    return () => {
      abort.abort();
      audio.pause();
      audio.removeAttribute("src");
      opened?.dispose();
    };
  }, [store, current]);

  useEffect(() => {
    if (audioRef.current) audioRef.current.volume = muted ? 0 : volume;
  }, [volume, muted]);

  const playQueue = useCallback((tracks: PlayableTrack[], startAt = 0) => {
    setQueue(tracks);
    setIndex(startAt);
  }, []);
  const next = useCallback(
    () => setIndex((value) => (value + 1 < queue.length ? value + 1 : value)),
    [queue.length],
  );
  const previous = useCallback(() => {
    const audio = audioRef.current;
    // Like most players: restart the track unless we are at its very start.
    if (audio && audio.currentTime > 3) audio.currentTime = 0;
    else setIndex((value) => Math.max(0, value - 1));
  }, []);
  const toggle = useCallback(() => {
    const audio = audioRef.current;
    if (!audio || !current) return;
    if (audio.paused) void audio.play().catch((cause) => setError(messageOf(cause)));
    else audio.pause();
  }, [current]);

  const state = useMemo<PlayerState>(
    () => ({ queue, index, current, isPlaying, loaded, error, playQueue, toggle, next, previous }),
    [queue, index, current, isPlaying, loaded, error, playQueue, toggle, next, previous],
  );

  return (
    <PlayerContext.Provider value={state}>
      {children}
      <audio
        ref={audioRef}
        onPlay={() => setIsPlaying(true)}
        onPause={() => setIsPlaying(false)}
        onTimeUpdate={(event) => setPosition(event.currentTarget.currentTime)}
        onDurationChange={(event) => {
          const value = event.currentTarget.duration;
          if (Number.isFinite(value) && value > 0) setDuration(value);
        }}
        onEnded={() => {
          if (index + 1 < queue.length) setIndex(index + 1);
        }}
      />
      {current && (
        <PlayerBar
          track={current}
          isPlaying={isPlaying}
          loaded={loaded}
          error={error}
          position={position}
          duration={duration}
          volume={muted ? 0 : volume}
          hasPrevious={index > 0}
          hasNext={index + 1 < queue.length}
          onToggle={toggle}
          onNext={next}
          onPrevious={previous}
          onSeek={(seconds) => {
            if (audioRef.current) audioRef.current.currentTime = seconds;
            setPosition(seconds);
          }}
          onVolume={(value) => {
            setMuted(false);
            setVolume(value);
          }}
          onToggleMute={() => setMuted((value) => !value)}
        />
      )}
    </PlayerContext.Provider>
  );
}

function PlayerBar(props: {
  track: PlayableTrack;
  isPlaying: boolean;
  loaded: number;
  error: string | undefined;
  position: number;
  duration: number;
  volume: number;
  hasPrevious: boolean;
  hasNext: boolean;
  onToggle(): void;
  onNext(): void;
  onPrevious(): void;
  onSeek(seconds: number): void;
  onVolume(value: number): void;
  onToggleMute(): void;
}) {
  const { track, loaded, error } = props;
  const status = error
    ? error
    : loaded < 1
      ? `${track.artist} · reading audio ${Math.round(loaded * 100)}%`
      : track.artist;
  return (
    <div className="rp-player" role="region" aria-label="Player">
      <HStack gap={3} vAlign="center" wrap="wrap" justify="between">
        <HStack gap={2} vAlign="center" className="rp-player-track">
          <CoverArt albumId={track.albumId} title={track.title} size="sm" />
          <VStack gap={0.5} className="rp-player-text">
            <Text weight="semibold" maxLines={1}>
              {track.title}
            </Text>
            <Text type="supporting" color="secondary" maxLines={1}>
              {status}
            </Text>
          </VStack>
        </HStack>
        <HStack gap={1} vAlign="center">
          <IconButton
            label="Previous"
            variant="ghost"
            icon={<Icon icon={SkipBack} size="sm" />}
            onClick={props.onPrevious}
            isDisabled={!props.hasPrevious && props.position < 3}
          />
          <IconButton
            label={props.isPlaying ? "Pause" : "Play"}
            variant="primary"
            icon={<Icon icon={props.isPlaying ? Pause : Play} size="sm" />}
            onClick={props.onToggle}
          />
          <IconButton
            label="Next"
            variant="ghost"
            icon={<Icon icon={SkipForward} size="sm" />}
            onClick={props.onNext}
            isDisabled={!props.hasNext}
          />
        </HStack>
        <HStack gap={2} vAlign="center" className="rp-player-seek">
          <Text type="supporting" color="secondary" hasTabularNumbers>
            {formatDuration(props.position * 1000)}
          </Text>
          <div className="rp-grow">
            <Slider
              label="Seek"
              isLabelHidden
              min={0}
              max={Math.max(1, Math.round(props.duration))}
              value={Math.min(Math.round(props.position), Math.round(props.duration))}
              valueDisplay="none"
              formatValue={(seconds) => formatDuration(seconds * 1000)}
              onChange={props.onSeek}
            />
          </div>
          <Text type="supporting" color="secondary" hasTabularNumbers>
            {formatDuration(props.duration * 1000)}
          </Text>
        </HStack>
        <HStack gap={1} vAlign="center" className="rp-player-volume">
          <IconButton
            label={props.volume === 0 ? "Unmute" : "Mute"}
            variant="ghost"
            icon={<Icon icon={props.volume === 0 ? VolumeX : Volume2} size="sm" />}
            onClick={props.onToggleMute}
          />
          <div className="rp-grow">
            <Slider
              label="Volume"
              isLabelHidden
              min={0}
              max={100}
              value={Math.round(props.volume * 100)}
              valueDisplay="none"
              onChange={(value) => props.onVolume(value / 100)}
            />
          </div>
        </HStack>
      </HStack>
    </div>
  );
}

function messageOf(cause: unknown): string {
  if (cause instanceof DOMException && cause.name === "NotAllowedError") {
    return "Press play to start audio.";
  }
  return cause instanceof Error ? cause.message : String(cause);
}
