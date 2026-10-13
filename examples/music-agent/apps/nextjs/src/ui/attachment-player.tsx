"use client";

import { Card } from "@astryxdesign/core/Card";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";

export type AttachmentSummary = {
  id: string;
  filename: string;
  mediaType: string;
  byteLength: number;
};

/**
 * An inline player for an audio attachment. The browser's audio element asks
 * for byte ranges as it plays and seeks; the route answers each one with a
 * partial read of the Jazz `bytes` column, so the full file is never loaded
 * just to start playback. (Astryx has no audio player, so this uses the
 * native control.)
 */
export function AttachmentPlayer({ attachment }: { attachment: AttachmentSummary }) {
  const isAudio = attachment.mediaType.startsWith("audio/");
  return (
    <Card padding={3} width="100%">
      <VStack gap={2}>
        <HStack gap={2} align="center">
          <Text weight="medium" maxLines={1} hasTruncateTooltip>
            {attachment.filename}
          </Text>
          <Text type="supporting" color="secondary">
            {formatBytes(attachment.byteLength)}
          </Text>
        </HStack>
        {isAudio && (
          <audio
            className="attachment-audio"
            controls
            preload="metadata"
            src={`/api/attachments/${attachment.id}`}
            aria-label={`Play ${attachment.filename}`}
          />
        )}
      </VStack>
    </Card>
  );
}

function formatBytes(bytes: number) {
  if (bytes >= 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  return `${Math.max(1, Math.round(bytes / 1000))} kB`;
}
