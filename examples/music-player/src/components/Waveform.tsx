import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { useAudioManager } from "@/lib/audio/AudioManager";
import { cn } from "@/lib/utils";
import { useCoState } from "jazz-tools/react";
import { useSyncExternalStore } from "react";

export function Waveform(props: {
  track: MusicTrack;
  height: number;
  className?: string;
  showProgress?: boolean;
}) {
  const { track, height } = props;
  const waveform = useCoState(
    MusicTrackWaveform,
    track.$jazz.refs.waveform?.id,
  );
  const audioManager = useAudioManager();
  const currentTime = useSyncExternalStore(
    (callback) => audioManager.on("timeUpdate", callback),
    () => audioManager.currentTime,
  );

  if (!waveform.$isLoaded) {
    return (
      <div
        style={{
          height,
        }}
      />
    );
  }

  const duration = track.duration;
  const waveformData = waveform.data;
  const barCount = waveformData.length;
  const activeBar = props.showProgress
    ? Math.ceil(barCount * (currentTime / duration))
    : -1;

  return (
    <div
      className={cn("flex justify-center items-end w-full", props.className)}
      style={{
        height,
      }}
    >
      {waveformData.map((value, i) => (
        <button
          type="button"
          key={i}
          className={cn(
            "w-1 transition-colors rounded-none rounded-t-lg min-h-1",
            activeBar >= i ? "bg-gray-800" : "bg-gray-400",
            "focus-visible:outline-black focus:outline-hidden",
          )}
          style={{
            height: height * value,
          }}
          aria-label={`Seek to ${(i / barCount) * duration} seconds`}
        />
      ))}
    </div>
  );
}
