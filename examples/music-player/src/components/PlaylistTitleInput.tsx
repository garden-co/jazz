import { Playlist } from "@/1_schema";
import { updatePlaylistTitle } from "@/4_actions";
import { cn } from "@/lib/utils";
import { useCoStateAndRef } from "jazz-tools/react";
import { ChangeEvent, useState } from "react";

export function PlaylistTitleInput({
  playlistId,
  className,
}: {
  playlistId: string | undefined;
  className?: string;
}) {
  const [playlistTitle, playlistRef] = useCoStateAndRef(Playlist, playlistId, {
    select: (playlist) => (playlist.$isLoaded ? playlist.title : ""),
  });
  const [isEditing, setIsEditing] = useState(false);
  const [localPlaylistTitle, setLocalPlaylistTitle] = useState("");

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalPlaylistTitle(evt.target.value);
  }

  function handleFoucsIn() {
    setIsEditing(true);
    setLocalPlaylistTitle(playlistTitle);
  }

  function handleFocusOut() {
    setIsEditing(false);
    setLocalPlaylistTitle("");
    if (playlistRef.current.$isLoaded) {
      updatePlaylistTitle(playlistRef.current, localPlaylistTitle);
    }
  }

  const inputValue = isEditing ? localPlaylistTitle : playlistTitle;

  return (
    <input
      value={inputValue}
      onChange={handleTitleChange}
      className={cn(
        "text-2xl font-bold text-blue-800 bg-transparent",
        className,
      )}
      onFocus={handleFoucsIn}
      onBlur={handleFocusOut}
      aria-label={`Playlist title`}
    />
  );
}
