import { MusicTrack } from "@/1_schema";
import { updateMusicTrackTitle } from "@/4_actions";
import { useCoStateAndRef } from "jazz-tools/react";
import { ChangeEvent, useState } from "react";

export function MusicTrackTitleInput({
  trackId,
}: {
  trackId: string | undefined;
}) {
  const [trackTitle, trackRef] = useCoStateAndRef(MusicTrack, trackId, {
    select: (track) => (track.$isLoaded ? track.title : ""),
  });
  const [isEditing, setIsEditing] = useState(false);
  const [localTrackTitle, setLocalTrackTitle] = useState("");

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalTrackTitle(evt.target.value);
  }

  function handleFoucsIn() {
    setIsEditing(true);
    setLocalTrackTitle(trackTitle);
  }

  function handleFocusOut() {
    setIsEditing(false);
    setLocalTrackTitle("");
    if (trackRef.current.$isLoaded) {
      updateMusicTrackTitle(trackRef.current, localTrackTitle);
    }
  }

  const inputValue = isEditing ? localTrackTitle : trackTitle;

  return (
    <div
      className="relative grow max-w-64"
      onClick={(evt) => evt.stopPropagation()}
    >
      <input
        className="absolute w-full h-full left-0 bg-transparent px-1"
        value={inputValue}
        onChange={handleTitleChange}
        spellCheck="false"
        onFocus={handleFoucsIn}
        onBlur={handleFocusOut}
        aria-label={`Edit track title: ${trackTitle}`}
      />
      <span className="opacity-0 px-1 w-fit pointer-events-none whitespace-pre">
        {inputValue}
      </span>
    </div>
  );
}
