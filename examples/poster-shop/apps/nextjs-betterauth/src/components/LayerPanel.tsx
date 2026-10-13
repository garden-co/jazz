"use client";

import {
  Button,
  Heading,
  HStack,
  IconButton,
  List,
  ListItem,
  TextInput,
  ToggleButton,
  VStack,
} from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { ChevronDown, ChevronUp, Eye, EyeOff, Lock, LockOpen, Pencil, Plus } from "lucide-react";
import { useState } from "react";
import { app, type Layer } from "@/schema";
import { nextZIndex, reorder } from "@/src/lib/poster";

/** Layers, top-most first, as in most design tools. */
export function LayerPanel({
  canvasId,
  canEdit,
  activeLayerId,
  onActiveLayerChange,
}: {
  canvasId: string;
  canEdit: boolean;
  activeLayerId: string | null;
  onActiveLayerChange: (id: string | null) => void;
}) {
  const db = useDb();
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "desc"));
  const [renaming, setRenaming] = useState<{ id: string; name: string } | null>(null);

  const addLayer = () => {
    const { value } = db.insert(app.layers, {
      canvasId,
      name: `Layer ${layers.length + 1}`,
      zIndex: nextZIndex(layers),
      visible: true,
      locked: false,
    });
    onActiveLayerChange(value.id);
  };

  const move = async (layer: Layer, direction: "up" | "down") => {
    const moves = reorder(layers, layer.id, direction);
    if (moves.length === 0) return;
    await db.transaction((tx) => {
      for (const next of moves) tx.update(app.layers, next.id, { zIndex: next.zIndex });
    });
  };

  const commitRename = () => {
    if (!renaming) return;
    const name = renaming.name.trim();
    if (name) db.update(app.layers, renaming.id, { name });
    setRenaming(null);
  };

  return (
    <VStack gap={3}>
      <HStack gap={2} vAlign="center" justify="between">
        <Heading level={2}>Layers</Heading>
        {canEdit && (
          <Button
            label="Add layer"
            size="sm"
            variant="secondary"
            icon={<Plus />}
            onClick={addLayer}
          />
        )}
      </HStack>
      <List density="compact" hasDividers>
        {layers.map((layer, index) =>
          renaming?.id === layer.id ? (
            <ListItem
              key={layer.id}
              label={
                <TextInput
                  label="Layer name"
                  isLabelHidden
                  size="sm"
                  value={renaming.name}
                  hasAutoFocus
                  onChange={(name) => setRenaming({ id: layer.id, name })}
                  onEnter={commitRename}
                  onBlur={commitRename}
                  onKeyDown={(event) => event.key === "Escape" && setRenaming(null)}
                />
              }
            />
          ) : (
            <ListItem
              key={layer.id}
              label={layer.name}
              isSelected={layer.id === activeLayerId}
              onClick={() => onActiveLayerChange(layer.id === activeLayerId ? null : layer.id)}
              endContent={
                <HStack gap={0.5} vAlign="center">
                  <ToggleButton
                    label={layer.visible ? "Hide layer" : "Show layer"}
                    tooltip={layer.visible ? "Hide layer" : "Show layer"}
                    isIconOnly
                    size="sm"
                    isPressed={!layer.visible}
                    isDisabled={!canEdit}
                    icon={layer.visible ? <Eye /> : <EyeOff />}
                    onPressedChange={(hidden) =>
                      db.update(app.layers, layer.id, { visible: !hidden })
                    }
                  />
                  <ToggleButton
                    label={layer.locked ? "Unlock layer" : "Lock layer"}
                    tooltip={layer.locked ? "Unlock layer" : "Lock layer"}
                    isIconOnly
                    size="sm"
                    isPressed={layer.locked}
                    isDisabled={!canEdit}
                    icon={layer.locked ? <Lock /> : <LockOpen />}
                    onPressedChange={(locked) => db.update(app.layers, layer.id, { locked })}
                  />
                  {canEdit && (
                    <>
                      <IconButton
                        label="Rename layer"
                        tooltip="Rename"
                        size="sm"
                        variant="ghost"
                        icon={<Pencil />}
                        onClick={() => setRenaming({ id: layer.id, name: layer.name })}
                      />
                      <IconButton
                        label="Move layer up"
                        tooltip="Move up"
                        size="sm"
                        variant="ghost"
                        icon={<ChevronUp />}
                        isDisabled={index === 0}
                        onClick={() => void move(layer, "up")}
                      />
                      <IconButton
                        label="Move layer down"
                        tooltip="Move down"
                        size="sm"
                        variant="ghost"
                        icon={<ChevronDown />}
                        isDisabled={index === layers.length - 1}
                        onClick={() => void move(layer, "down")}
                      />
                    </>
                  )}
                </HStack>
              }
            />
          ),
        )}
      </List>
    </VStack>
  );
}
