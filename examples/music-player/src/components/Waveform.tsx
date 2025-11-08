import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { usePlayerCurrentTime } from "@/lib/audio/usePlayerCurrentTime";
import { cn, shallowEqual } from "@/lib/utils";
import { useCoState } from "jazz-tools/react";

export function Waveform(props: {
  trackId: string;
  height: number;
  className?: string;
  showProgress?: boolean;
}) {
  const { height } = props;
  const { duration, waveformId } = useCoState(MusicTrack, props.trackId, {
    select: (track) =>
      track.$isLoaded
        ? {
            duration: track.duration,
            waveformId: track.waveform?.$jazz.id,
          }
        : {
            duration: undefined,
            waveformId: undefined,
          },
    equalityFn: shallowEqual,
  });
  const waveform = useCoState(MusicTrackWaveform, waveformId);
  const currentTime = usePlayerCurrentTime();

  if (!waveform.$isLoaded || duration === undefined) {
    return (
      <div
        style={{
          height,
        }}
      />
    );
  }

  const waveformData = waveform.data;
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
