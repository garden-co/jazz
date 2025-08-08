import { Loaded } from "jazz-tools";
import { useCoState } from "jazz-tools/react";
import { MusicTrack, MusicTrackWaveform } from "@/1_schema";
import { usePlayerCurrentTime } from "@/lib/audio/usePlayerCurrentTime";
import { cn } from "@/lib/utils";

export function Waveform(props: {
  track: Loaded<typeof MusicTrack>;
  height: number;
  className?: string;
}) {
  const { track, height } = props;
  const waveformData = useCoState(
    MusicTrackWaveform,
    track._refs.waveform?.id,
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
  const activeBar = Math.ceil(barCount * (currentTime.value / duration));

  function seek(i: number) {
    currentTime.setValue((i / barCount) * duration);
  }

  return (
    <div
      className={cn("flex justify-center items-end w-full", props.className)}
      style={{
        height,
        gap: 1,
      }}
    >
      {waveformData.map((value, i) => (
        <button
          type="button"
          key={i}
          onClick={() => seek(i)}
          className={cn(
            "w-1 transition-colors rounded-none rounded-t-lg min-h-1",
            activeBar >= i ? "bg-gray-500" : "bg-gray-300",
            "hover:bg-black hover:border hover:border-solid hover:border-black",
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
