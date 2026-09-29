"use client";

import {
  Button,
  EmptyState,
  Heading,
  HStack,
  List,
  ListItem,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { History } from "lucide-react";
import { useState } from "react";
import { app } from "@/schema";
import { takeSnapshot } from "@/src/lib/poster";

/**
 * Named snapshots of the poster. The list reads labels only; a snapshot's
 * JSON is read when it is previewed.
 */
export function CheckpointShelf({
  canvasId,
  canAdmin,
  previewCheckpointId,
  onPreview,
}: {
  canvasId: string;
  canAdmin: boolean;
  previewCheckpointId: string | null;
  onPreview: (id: string | null) => void;
}) {
  const db = useDb();
  const { data: checkpoints = [] } = useAll(
    app.checkpoints.where({ canvasId }).select("id", "label", "$createdAt"),
  );
  const ordered = [...checkpoints].sort(
    (a, b) => Number(b.$createdAt ?? 0) - Number(a.$createdAt ?? 0),
  );
  const [label, setLabel] = useState("");
  const [saving, setSaving] = useState(false);

  const save = async () => {
    setSaving(true);
    try {
      // Read the live rows at save time; the snapshot is immutable afterwards.
      const [layers, shapes] = await Promise.all([
        db.all(app.layers.where({ canvasId })),
        db.all(app.shapes.where({ canvasId })),
      ]);
      db.insert(app.checkpoints, {
        canvasId,
        label: label.trim() || `Checkpoint ${checkpoints.length + 1}`,
        branch: "main",
        snapshot: takeSnapshot(layers, shapes),
      });
      setLabel("");
    } finally {
      setSaving(false);
    }
  };

  return (
    <VStack gap={4}>
      <Heading level={2}>History</Heading>
      {canAdmin && (
        <HStack gap={2} vAlign="end">
          <TextInput
            label="Checkpoint name"
            placeholder={`Checkpoint ${checkpoints.length + 1}`}
            value={label}
            onChange={setLabel}
            onEnter={() => void save()}
            width="100%"
          />
          <Button label="Save" variant="primary" isLoading={saving} onClick={() => void save()} />
        </HStack>
      )}
      {ordered.length === 0 ? (
        <EmptyState
          isCompact
          headingLevel={3}
          icon={<History />}
          title="No checkpoints yet"
          description="Admins can save named snapshots of the poster to look back at later."
        />
      ) : (
        <List density="compact" hasDividers>
          {ordered.map((checkpoint) => {
            const previewing = checkpoint.id === previewCheckpointId;
            return (
              <ListItem
                key={checkpoint.id}
                label={checkpoint.label}
                description={
                  checkpoint.$createdAt ? (
                    <Text type="supporting" color="secondary">
                      {formatTime(checkpoint.$createdAt)}
                    </Text>
                  ) : undefined
                }
                isSelected={previewing}
                endContent={
                  <Button
                    label={previewing ? "Back to live" : "Preview"}
                    size="sm"
                    variant={previewing ? "primary" : "secondary"}
                    onClick={() => onPreview(previewing ? null : checkpoint.id)}
                  />
                }
              />
            );
          })}
        </List>
      )}
    </VStack>
  );
}

function formatTime(value: unknown) {
  const date = value instanceof Date ? value : new Date(Number(value));
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}
