"use client";

import {
  EmptyState,
  Heading,
  HStack,
  NumberInput,
  Selector,
  Slider,
  Text,
  TextInput,
  ToggleButton,
  ToggleButtonGroup,
  VStack,
} from "@astryxdesign/core";
import { useAll, useDb, useOne } from "jazz-tools/react";
import { MousePointerClick } from "lucide-react";
import { app } from "@/schema";
import { MIN_SHAPE_SIZE, PALETTE, fillColor, nextZIndex } from "@/src/lib/poster";

const KIND_LABELS = { rect: "Rectangle", ellipse: "Ellipse", text: "Text", image: "Image" };

/** Properties of the selected shape. It subscribes to that one row only. */
export function Inspector({
  canvasId,
  shapeId,
  canEdit,
}: {
  canvasId: string;
  shapeId: string | null;
  canEdit: boolean;
}) {
  const db = useDb();
  const { data: shape } = useOne(shapeId ? app.shapes.where({ id: shapeId }) : undefined);
  const { data: layers = [] } = useAll(app.layers.where({ canvasId }).orderBy("zIndex", "desc"));
  if (!shape)
    return (
      <EmptyState
        isCompact
        headingLevel={2}
        icon={<MousePointerClick />}
        title="Nothing selected"
        description={
          canEdit
            ? "Select a shape on the poster, or add one from the toolbar."
            : "You can look around, but this poster is read-only for you."
        }
      />
    );

  const update = (values: Partial<typeof shape>) => db.update(app.shapes, shape.id, values);
  const movableLayers = layers.filter((layer) => !layer.locked || layer.id === shape.layerId);

  return (
    <VStack gap={4}>
      <Heading level={2}>{KIND_LABELS[shape.kind]}</Heading>
      {shape.kind === "text" && (
        <TextInput
          label="Text"
          value={shape.text ?? ""}
          isDisabled={!canEdit}
          onChange={(text) => update({ text })}
        />
      )}
      {shape.kind !== "image" && (
        <VStack gap={2}>
          <Text type="label">Colour</Text>
          <ToggleButtonGroup
            label="Colour"
            value={shape.fill}
            size="sm"
            isDisabled={!canEdit}
            onChange={(fill) => fill && update({ fill })}
          >
            <HStack gap={1} wrap="wrap">
              {PALETTE.map((colour) => (
                <ToggleButton
                  key={colour.key}
                  value={colour.key}
                  label={colour.label}
                  tooltip={colour.label}
                  isIconOnly
                  size="sm"
                  icon={<Swatch fill={fillColor(colour.key)} />}
                />
              ))}
            </HStack>
          </ToggleButtonGroup>
        </VStack>
      )}
      <div className="inspector-pair">
        <NumberInput
          label="X"
          value={shape.x}
          isDisabled={!canEdit}
          onChange={(x) => update({ x })}
        />
        <NumberInput
          label="Y"
          value={shape.y}
          isDisabled={!canEdit}
          onChange={(y) => update({ y })}
        />
      </div>
      <div className="inspector-pair">
        <NumberInput
          label="Width"
          value={shape.width}
          min={MIN_SHAPE_SIZE}
          isDisabled={!canEdit}
          onChange={(width) => update({ width })}
        />
        <NumberInput
          label="Height"
          value={shape.height}
          min={MIN_SHAPE_SIZE}
          isDisabled={!canEdit}
          onChange={(height) => update({ height })}
        />
      </div>
      <Slider
        label="Rotation"
        value={shape.rotation}
        min={-180}
        max={180}
        step={1}
        valueDisplay="text"
        formatValue={(value: number) => `${value}°`}
        isDisabled={!canEdit}
        onChange={(rotation: number) => update({ rotation })}
      />
      <Selector
        label="Layer"
        value={shape.layerId}
        isDisabled={!canEdit}
        options={movableLayers.map((layer) => ({ value: layer.id, label: layer.name }))}
        onChange={async (layerId) => {
          if (layerId === shape.layerId) return;
          const siblings = await db.all(app.shapes.where({ layerId }).select("zIndex"));
          update({ layerId, zIndex: nextZIndex(siblings) });
        }}
      />
    </VStack>
  );
}

/** A colour chip for the palette buttons; the colour itself is a token. */
function Swatch({ fill }: { fill: string }) {
  return (
    <svg viewBox="0 0 16 16" aria-hidden="true" className="swatch">
      <circle cx={8} cy={8} r={7} fill={fill} />
    </svg>
  );
}
