"use client";

import { ListPlus, Play, Upload } from "lucide-react";
import { useAll, useDb } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { DropdownMenu } from "@astryxdesign/core/DropdownMenu";
import { Heading } from "@astryxdesign/core/Heading";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Table, pixel, proportional } from "@astryxdesign/core/Table";
import { Text } from "@astryxdesign/core/Text";
import { app } from "../schema";
import type { PlayableTrack } from "../src/audio-stream";
import { ALBUM_TRACK_LIMIT, positionBetween } from "../src/record-player";
import { CoverArt } from "./cover-art";
import { formatBytes, formatDuration } from "./format";
import { useStore, usePlaylists, type Album } from "./library-data";
import { usePlayer } from "./player";

type TrackRow = PlayableTrack & { ordinal: number };

/** Metadata-only track list: audio bytes are read only when a track plays. */
export function useAlbumTracks(album: Album | undefined): TrackRow[] | undefined {
  const tracks = useAll(
    album
      ? app.tracks
          .where({ album_id: album.id })
          .orderBy("ordinal", "asc")
          .limit(ALBUM_TRACK_LIMIT)
          .select("title", "ordinal", "duration_ms", "audio_mime", "audio_byte_length")
      : undefined,
  );
  return tracks.data?.map((track) => ({
    id: track.id,
    title: track.title,
    ordinal: track.ordinal,
    durationMs: track.duration_ms,
    mimeType: track.audio_mime,
    byteLength: track.audio_byte_length,
    albumId: album!.id,
    artist: album!.artist,
    hasCover: Boolean(album!.cover_mime),
  }));
}

export function AlbumView({ album, onAddTracks }: { album: Album; onAddTracks(): void }) {
  const tracks = useAlbumTracks(album);
  const player = usePlayer();
  const totalMs = tracks?.reduce((sum, track) => sum + track.durationMs, 0) ?? 0;

  return (
    <VStack gap={4}>
      <HStack gap={4} vAlign="end" wrap="wrap">
        <CoverArt
          albumId={album.id}
          title={album.title}
          hasCover={Boolean(album.cover_mime)}
          size="md"
        />
        <VStack gap={2}>
          <VStack gap={0.5}>
            <Heading level={2}>{album.title}</Heading>
            <Text color="secondary">
              {album.artist} · {tracks?.length ?? 0} tracks · {formatDuration(totalMs)}
            </Text>
          </VStack>
          <HStack gap={2} wrap="wrap">
            <Button
              label="Play album"
              variant="primary"
              icon={<Icon icon={Play} size="sm" />}
              isDisabled={!tracks?.length}
              onClick={() => tracks && player.playQueue(tracks)}
            />
            <Button
              label="Add tracks"
              variant="secondary"
              icon={<Icon icon={Upload} size="sm" />}
              onClick={onAddTracks}
            />
          </HStack>
        </VStack>
      </HStack>
      <Table<TrackRow>
        data={tracks ?? []}
        idKey="id"
        density="compact"
        hasHover
        columns={[
          {
            key: "ordinal",
            header: "#",
            width: pixel(48),
            renderCell: (track) => (
              <Text color="secondary" hasTabularNumbers>
                {track.ordinal}
              </Text>
            ),
          },
          {
            key: "title",
            header: "Title",
            width: proportional(3, { minWidth: 140 }),
            renderCell: (track) => (
              <HStack gap={2} vAlign="center">
                <Text maxLines={1}>{track.title}</Text>
                {player.current?.id === track.id && <Badge variant="info" label="Playing" />}
              </HStack>
            ),
          },
          {
            key: "size",
            header: "Size",
            width: pixel(88),
            renderCell: (track) => (
              <Text color="secondary" hasTabularNumbers>
                {track.byteLength ? formatBytes(track.byteLength) : "–"}
              </Text>
            ),
          },
          {
            key: "duration",
            header: "Length",
            width: pixel(72),
            align: "end",
            renderCell: (track) => (
              <Text color="secondary" hasTabularNumbers>
                {formatDuration(track.durationMs)}
              </Text>
            ),
          },
          {
            key: "actions",
            header: "",
            width: pixel(96),
            align: "end",
            renderCell: (track) => (
              <HStack gap={0.5} justify="end">
                <IconButton
                  label={`Play ${track.title}`}
                  variant="ghost"
                  size="sm"
                  icon={<Icon icon={Play} size="sm" />}
                  onClick={() => tracks && player.playQueue(tracks, tracks.indexOf(track))}
                />
                <AddToPlaylist trackId={track.id} trackTitle={track.title} />
              </HStack>
            ),
          },
        ]}
      />
    </VStack>
  );
}

function AddToPlaylist({ trackId, trackTitle }: { trackId: string; trackTitle: string }) {
  const db = useDb();
  const store = useStore();
  const { playlists } = usePlaylists();
  const editable = playlists.filter((playlist) => playlist.canEdit);

  async function add(playlistId: string) {
    const [last] = await db.all(
      app.playlist_entries
        .where({ playlist_id: playlistId })
        .orderBy("position", "desc")
        .limit(1)
        .select("position"),
    );
    store.addToPlaylist(playlistId, trackId, positionBetween(last?.position, undefined));
  }

  return (
    <DropdownMenu
      button={{
        label: `Add ${trackTitle} to a playlist`,
        variant: "ghost",
        size: "sm",
        isIconOnly: true,
        icon: <Icon icon={ListPlus} size="sm" />,
      }}
      hasChevron={false}
      alignment="end"
      items={
        editable.length > 0
          ? editable.map((playlist) => ({
              id: playlist.id,
              label: playlist.name,
              onClick: () => void add(playlist.id),
            }))
          : [{ id: "none", label: "Create a playlist first", isDisabled: true }]
      }
    />
  );
}
