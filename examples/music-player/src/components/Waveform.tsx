import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { usePlayerCurrentTime } from "@/lib/audio/usePlayerCurrentTime";
import { cn } from "@/lib/utils";
import { Loaded } from "jazz-tools";
import { useCoState } from "jazz-tools/react";

export function Waveform(props: {
  track: Loaded<typeof MusicTrack>;
  height: number;
  className?: string;
  showProgress?: boolean;
}) {
  const { track, height } = props;
  const waveformData = useCoState(
    MusicTrackWaveform,
    track.$jazz.refs.waveform?.id,
  )?.data;
  const duration = track.duration;

  const currentTime = usePlayerCurrentTime();

  if (!waveformData) {
    return (
      <div
        style={{
          height,
        }}
      />
    );
  }

  const barCount = waveformData.length;
  const activeBar = props.showProgress
    ? Math.ceil(barCount * (currentTime.value / duration))
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
