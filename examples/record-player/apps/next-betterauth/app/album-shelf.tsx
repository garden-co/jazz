"use client";

import { useEffect, useRef, type KeyboardEvent } from "react";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/Stack";
import type { Album } from "./library-data";
import { CoverArt } from "./cover-art";

/**
 * A CoverFlow-style shelf: a horizontal scroller whose covers turn towards the
 * selected album. Arrow keys move the selection; the page never scrolls
 * vertically inside it.
 */
export function AlbumShelf({
  albums,
  selectedId,
  onSelect,
}: {
  albums: Album[];
  selectedId: string | undefined;
  onSelect(id: string): void;
}) {
  const shelfRef = useRef<HTMLDivElement>(null);
  const selectedIndex = Math.max(
    0,
    albums.findIndex((album) => album.id === selectedId),
  );

  useEffect(() => {
    const selected = shelfRef.current?.querySelector<HTMLElement>("[aria-selected='true']");
    selected?.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
  }, [selectedId]);

  function onKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    const step = event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const next = albums[Math.min(albums.length - 1, Math.max(0, selectedIndex + step))];
    if (next) {
      onSelect(next.id);
      shelfRef.current?.querySelector<HTMLElement>(`[data-album='${next.id}']`)?.focus();
    }
  }

  return (
    <div
      ref={shelfRef}
      className="rp-shelf"
      role="listbox"
      aria-label="Albums"
      aria-orientation="horizontal"
      onKeyDown={onKeyDown}
    >
      {albums.map((album, index) => {
        const offset = index - selectedIndex;
        const side = offset === 0 ? "center" : offset < 0 ? "left" : "right";
        return (
          <button
            key={album.id}
            type="button"
            role="option"
            aria-selected={offset === 0}
            tabIndex={offset === 0 ? 0 : -1}
            data-album={album.id}
            data-side={side}
            className="rp-shelf-item"
            onClick={() => onSelect(album.id)}
          >
            <CoverArt
              albumId={album.id}
              title={album.title}
              hasCover={Boolean(album.cover_mime)}
              size="lg"
            />
            <VStack gap={0.5}>
              <Text weight="semibold" maxLines={1}>
                {album.title}
              </Text>
              <Text type="supporting" color="secondary" maxLines={1}>
                {album.artist}
              </Text>
            </VStack>
          </button>
        );
      })}
    </div>
  );
}
