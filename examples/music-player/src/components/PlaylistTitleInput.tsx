import { Playlist } from "@/1_schema";
import { updatePlaylistTitle } from "@/4_actions";
import { cn } from "@/lib/utils";
import { useSuspenseCoState } from "jazz-tools/react-core";
import { ChangeEvent, useState } from "react";

export function PlaylistTitleInput({
  playlistId,
  className,
}: {
  playlistId: string | undefined;
  className?: string;
}) {
  const playlist = useSuspenseCoState(Playlist, playlistId);
  const [isEditing, setIsEditing] = useState(false);
  const [localPlaylistTitle, setLocalPlaylistTitle] = useState("");

  function handleTitleChange(evt: ChangeEvent<HTMLInputElement>) {
    setLocalPlaylistTitle(evt.target.value);
  }

  const playlistTitle = playlist.title;

  function handleFoucsIn() {
    setIsEditing(true);
    setLocalPlaylistTitle(playlistTitle);
  }

  function handleFocusOut() {
    setIsEditing(false);
    setLocalPlaylistTitle("");
    playlist.$isLoaded && updatePlaylistTitle(playlist, localPlaylistTitle);
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
