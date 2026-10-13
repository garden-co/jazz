"use client";

import { useEffect, useMemo, useState } from "react";
import { Play, Share2, Trash2 } from "lucide-react";
import { useAll } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Heading } from "@astryxdesign/core/Heading";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { SideNav, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Table, pixel, proportional } from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app } from "../schema";
import type { PlayableTrack } from "../src/audio-stream";
import { positionBetween } from "../src/record-player";
import { countLabel, formatDuration } from "./format";
import { FIRST_READ, usePlaylists, useStore, type PlaylistSummary } from "./library-data";
import { usePlayer } from "./player";
import { ShareDialog } from "./share-dialog";

const ROLE_LABEL = { owner: "Owner", editor: "Editor", listener: "Listener" } as const;

export function Playlists() {
  const store = useStore();
  const { playlists, isLoading } = usePlaylists();
  const [selectedId, setSelectedId] = useState<string>();
  const selected = playlists.find((playlist) => playlist.id === selectedId) ?? playlists[0];

  function create() {
    setSelectedId(store.createPlaylist("New playlist"));
  }

  if (!isLoading && playlists.length === 0) {
    return (
      <EmptyState
        title="No playlists yet"
        description="Create one, add tracks from the library, then invite listeners or editors."
        actions={<Button label="Create playlist" variant="primary" onClick={create} />}
      />
    );
  }

  return (
    <div className="rp-split">
      {/* Choosing a playlist navigates the detail pane, so the list is a
          SideNav: its items are nav rows with aria-current on the open one. */}
      <SideNav
        topContent={
          <Button label="Create playlist" variant="secondary" width="100%" onClick={create} />
        }
      >
        <SideNavSection title="Your playlists">
          {playlists.map((playlist) => (
            <SideNavItem
              key={playlist.id}
              label={playlist.name}
              endContent={
                playlist.role === "owner" ? undefined : (
                  <Badge variant="neutral" label={ROLE_LABEL[playlist.role]} />
                )
              }
              isSelected={playlist.id === selected?.id}
              onClick={() => setSelectedId(playlist.id)}
            />
          ))}
        </SideNavSection>
      </SideNav>
      {selected && <PlaylistDetail key={selected.id} playlist={selected} />}
    </div>
  );
}

type EntryRow = {
  id: string;
  position: number;
  track: PlayableTrack & { albumTitle: string };
};

function PlaylistDetail({ playlist }: { playlist: PlaylistSummary }) {
  const store = useStore();
  const player = usePlayer();
  const [isSharing, setSharing] = useState(false);
  const entries = useAll(
    app.playlist_entries
      .where({ playlist_id: playlist.id })
      .orderBy("position", "asc")
      .limit(500)
      .include({
        track: app.tracks
          .select("album_id", "title", "duration_ms", "audio_mime", "audio_byte_length")
          .include({ album: app.albums.select("title", "artist", "cover_mime") }),
      }),
    FIRST_READ,
  );

  const rows = useMemo<EntryRow[]>(
    () =>
      (entries.data ?? []).flatMap((entry) => {
        // A track can be missing while it syncs; show it once it arrives.
        if (!entry.track) return [];
        const album = entry.track.album;
        return [
          {
            id: entry.id,
            position: entry.position,
            track: {
              id: entry.track.id,
              title: entry.track.title,
              albumId: entry.track.album_id,
              albumTitle: album?.title ?? "",
              artist: album?.artist ?? "",
              hasCover: Boolean(album?.cover_mime),
              durationMs: entry.track.duration_ms,
              mimeType: entry.track.audio_mime,
              byteLength: entry.track.audio_byte_length,
            },
          },
        ];
      }),
    [entries.data],
  );

  function move(index: number, by: -1 | 1) {
    const entry = rows[index]!;
    const position =
      by < 0
        ? positionBetween(rows[index - 2]?.position, rows[index - 1]!.position)
        : positionBetween(rows[index + 1]!.position, rows[index + 2]?.position);
    store.moveEntry(entry.id, position);
  }

  const totalMs = rows.reduce((sum, row) => sum + row.track.durationMs, 0);
  const queue = rows.map((row) => row.track);

  return (
    <VStack gap={4}>
      <VStack gap={2}>
        {playlist.role === "owner" ? (
          <PlaylistName playlistId={playlist.id} name={playlist.name} />
        ) : (
          <Heading level={2}>{playlist.name}</Heading>
        )}
        <HStack gap={2} vAlign="center" wrap="wrap">
          <Badge
            variant={playlist.role === "listener" ? "neutral" : "info"}
            label={ROLE_LABEL[playlist.role]}
          />
          <Text color="secondary">
            {countLabel(rows.length, "track")} · {formatDuration(totalMs)}
          </Text>
        </HStack>
        <HStack gap={2} wrap="wrap">
          <Button
            label="Play"
            variant="primary"
            icon={<Icon icon={Play} size="sm" />}
            isDisabled={rows.length === 0}
            onClick={() => player.playQueue(queue)}
          />
          {playlist.role === "owner" && (
            <Button
              label="Share"
              variant="secondary"
              icon={<Icon icon={Share2} size="sm" />}
              onClick={() => setSharing(true)}
            />
          )}
        </HStack>
      </VStack>
      {rows.length === 0 ? (
        <Text color="secondary">
          {playlist.canEdit
            ? "Add tracks from an album in the library."
            : "The owner has not added any tracks yet."}
        </Text>
      ) : (
        <Table<EntryRow>
          data={rows}
          idKey="id"
          density="compact"
          hasHover
          columns={[
            {
              key: "title",
              header: "Title",
              width: proportional(3, { minWidth: 140 }),
              renderCell: (row) => (
                <VStack gap={0.5}>
                  <Text maxLines={1}>{row.track.title}</Text>
                  <Text type="supporting" color="secondary" maxLines={1}>
                    {row.track.artist} · {row.track.albumTitle}
                  </Text>
                </VStack>
              ),
            },
            {
              key: "duration",
              header: "Length",
              width: pixel(72),
              align: "end",
              renderCell: (row) => (
                <Text color="secondary" hasTabularNumbers>
                  {formatDuration(row.track.durationMs)}
                </Text>
              ),
            },
            {
              key: "actions",
              header: "",
              width: pixel(playlist.canEdit ? 152 : 48),
              align: "end",
              renderCell: (row) => {
                const index = rows.indexOf(row);
                return (
                  <HStack gap={0.5} justify="end">
                    <IconButton
                      label={`Play ${row.track.title}`}
                      variant="ghost"
                      size="sm"
                      icon={<Icon icon={Play} size="sm" />}
                      onClick={() => player.playQueue(queue, index)}
                    />
                    {playlist.canEdit && (
                      <>
                        <IconButton
                          label="Move up"
                          variant="ghost"
                          size="sm"
                          icon={<Icon icon="arrowUp" size="sm" />}
                          isDisabled={index === 0}
                          onClick={() => move(index, -1)}
                        />
                        <IconButton
                          label="Move down"
                          variant="ghost"
                          size="sm"
                          icon={<Icon icon="arrowDown" size="sm" />}
                          isDisabled={index === rows.length - 1}
                          onClick={() => move(index, 1)}
                        />
                        <IconButton
                          label={`Remove ${row.track.title}`}
                          variant="ghost"
                          size="sm"
                          icon={<Icon icon={Trash2} size="sm" />}
                          onClick={() => store.removeEntry(row.id)}
                        />
                      </>
                    )}
                  </HStack>
                );
              },
            },
          ]}
        />
      )}
      <ShareDialog playlist={playlist} isOpen={isSharing} onClose={() => setSharing(false)} />
    </VStack>
  );
}

/** Owners rename in place; the change syncs to every listener. */
function PlaylistName({ playlistId, name }: { playlistId: string; name: string }) {
  const store = useStore();
  const [draft, setDraft] = useState(name);
  useEffect(() => setDraft(name), [name]);
  const commit = () => {
    const next = draft.trim();
    if (next && next !== name) store.renamePlaylist(playlistId, next);
    else setDraft(name);
  };
  return (
    <div className="rp-title-input" onBlur={commit}>
      <TextInput
        label="Playlist name"
        isLabelHidden
        value={draft}
        onChange={setDraft}
        onEnter={commit}
      />
    </div>
  );
}
