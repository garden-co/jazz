"use client";

import {
  Banner,
  Button,
  EmptyState,
  FileInput,
  Grid,
  Heading,
  HStack,
  Text,
  Thumbnail,
  VStack,
} from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { ImagePlus } from "lucide-react";
import { useState } from "react";
import { app } from "@/schema";
import { nextZIndex } from "@/src/lib/poster";
import { useAssetUrl } from "@/src/lib/use-asset-url";

const MAX_UPLOAD_BYTES = 8 * 1024 * 1024;

/**
 * Images for this poster. The listing selects metadata columns only, so
 * opening the shelf never reads upload bytes; each thumbnail reads its own
 * bytes through typed byte-range selections.
 */
export function AssetShelf({
  canvasId,
  canEdit,
  posterWidth,
  posterHeight,
  activeLayerId,
  onPlaced,
}: {
  canvasId: string;
  posterWidth: number;
  posterHeight: number;
  canEdit: boolean;
  activeLayerId: string | null;
  onPlaced: (shapeId: string) => void;
}) {
  const db = useDb();
  const { data: assets = [] } = useAll(
    app.assets
      .where({ canvasId })
      .orderBy("name", "asc")
      .select("id", "name", "mimeType", "byteLength", "width", "height"),
  );
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "desc"));
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const upload = async (files: File | File[] | null) => {
    const list = files ? (Array.isArray(files) ? files : [files]) : [];
    if (list.length === 0) return;
    setUploading(true);
    setError(null);
    try {
      for (const file of list) {
        const { width, height } = await imageSize(file);
        // Stream the file into the `bytes` large value; the app never holds
        // the whole upload as one Uint8Array.
        const write = await db.insertStreaming(app.assets, {
          canvasId,
          name: file.name,
          mimeType: file.type || "application/octet-stream",
          byteLength: file.size,
          width,
          height,
          bytes: file.stream(),
        });
        await write.wait({ tier: "local" });
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setUploading(false);
    }
  };

  const place = async (asset: (typeof assets)[number]) => {
    const layer =
      layers.find((candidate) => candidate.id === activeLayerId && !candidate.locked) ??
      layers.find((candidate) => candidate.visible && !candidate.locked);
    if (!layer) {
      setError("Unlock a layer to place images on the poster.");
      return;
    }
    const siblings = await db.all(app.shapes.where({ layerId: layer.id }).select("zIndex"));
    const width = Math.round(posterWidth / 2);
    const height = Math.round((width * asset.height) / Math.max(1, asset.width));
    const { value } = db.insert(app.shapes, {
      canvasId,
      layerId: layer.id,
      assetId: asset.id,
      kind: "image",
      x: (posterWidth - width) / 2,
      y: Math.max(0, (posterHeight - height) / 2),
      width,
      height,
      rotation: 0,
      zIndex: nextZIndex(siblings),
      fill: "paper",
    });
    onPlaced(value.id);
  };

  return (
    <VStack gap={4}>
      <Heading level={2}>Assets</Heading>
      {canEdit && (
        <FileInput
          label="Upload images"
          isLabelHidden
          mode="dropzone"
          accept="image/png,image/jpeg,image/gif,image/webp"
          isMultiple
          maxSize={MAX_UPLOAD_BYTES}
          isLoading={uploading}
          description="PNG, JPEG, GIF or WebP up to 8 MB"
          value={null}
          onChange={(files) => void upload(files)}
        />
      )}
      {error && <Banner status="error" title="Could not add the image" description={error} />}
      {assets.length === 0 ? (
        <EmptyState
          isCompact
          headingLevel={3}
          icon={<ImagePlus />}
          title="No images yet"
          description={
            canEdit
              ? "Uploaded images sync to everyone on this poster."
              : "Images added by editors appear here."
          }
        />
      ) : (
        <Grid role="list" aria-label="Images" columns={{ minWidth: 128 }} gap={3}>
          {assets.map((asset) => (
            <div role="listitem" key={asset.id}>
              <AssetTile asset={asset} canPlace={canEdit} onPlace={() => void place(asset)} />
            </div>
          ))}
        </Grid>
      )}
    </VStack>
  );
}

function AssetTile({
  asset,
  canPlace,
  onPlace,
}: {
  asset: { id: string; name: string; mimeType: string; byteLength: number };
  canPlace: boolean;
  onPlace: () => void;
}) {
  const url = useAssetUrl(asset);
  return (
    <VStack gap={1}>
      <Thumbnail src={url ?? undefined} alt={asset.name} isLoading={!url} label={asset.name} />
      <Text type="supporting" maxLines={1} hasTruncateTooltip>
        {asset.name}
      </Text>
      <HStack gap={1} vAlign="center" justify="between">
        <Text type="supporting" color="secondary" hasTabularNumbers>
          {formatBytes(asset.byteLength)}
        </Text>
        {canPlace && <Button label="Place" size="sm" variant="secondary" onClick={onPlace} />}
      </HStack>
    </VStack>
  );
}

async function imageSize(file: File): Promise<{ width: number; height: number }> {
  const bitmap = await createImageBitmap(file);
  try {
    return { width: bitmap.width, height: bitmap.height };
  } finally {
    bitmap.close();
  }
}

function formatBytes(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
