"use client";

import {
  Badge,
  Banner,
  Button,
  EmptyState,
  Heading,
  HStack,
  SegmentedControl,
  SegmentedControlItem,
  Selector,
  Spinner,
  VStack,
} from "@astryxdesign/core";
import { useAll, useSession } from "jazz-tools/react";
import { useState, type ReactNode } from "react";
import { app } from "@/schema";
import { AssetShelf } from "@/src/components/AssetShelf";
import { CanvasSurface } from "@/src/components/CanvasSurface";
import { CheckpointShelf } from "@/src/components/CheckpointShelf";
import { CollaboratorAvatars } from "@/src/components/CollaboratorCursors";
import { Inspector } from "@/src/components/Inspector";
import { InviteButton } from "@/src/components/InviteDialog";
import { LayerPanel } from "@/src/components/LayerPanel";
import { authClient } from "@/src/lib/auth-client";
import { roleForActiveCanvas } from "@/src/lib/identity";

export function PosterShopApp({
  initialCanvasId,
  notice,
}: {
  initialCanvasId?: string | null;
  /** Shown above the studio, for example when an invite link was not valid. */
  notice?: ReactNode;
}) {
  return <PosterStudio initialCanvasId={initialCanvasId ?? null} notice={notice} />;
}

type SidePanel = "design" | "assets" | "history";

/** The shell only reads canvas metadata and owns selection state. Child
 * surfaces keep independent Jazz subscriptions, so a cursor or asset update
 * cannot invalidate the shape renderer. */
export function PosterStudio({
  initialCanvasId,
  notice,
}: {
  initialCanvasId: string | null;
  notice?: ReactNode;
}) {
  const session = useSession();
  const { data: authSession } = authClient.useSession();
  const { data: canvases, error: canvasesError } = useAll(app.canvases);
  const [activeId, setActiveId] = useState<string | null>(initialCanvasId);
  const [selectedShapeId, setSelectedShapeId] = useState<string | null>(null);
  const [activeLayerId, setActiveLayerId] = useState<string | null>(null);
  const [previewCheckpointId, setPreviewCheckpointId] = useState<string | null>(null);
  const [panel, setPanel] = useState<SidePanel>("design");
  const active = canvases?.find((canvas) => canvas.id === activeId) ?? canvases?.[0];
  const author = session?.user.account ?? null;
  const { data: memberships = [] } = useAll(app.canvasMembers);
  const role = roleForActiveCanvas(memberships, active?.id, author);
  const canEdit = (role === "editor" || role === "admin") && !previewCheckpointId;
  const canAdmin = role === "admin";
  const displayName = authSession?.user.name ?? "Guest";

  if (canvasesError)
    return (
      <Banner
        status="error"
        title="Could not load your posters"
        description={canvasesError.message}
      />
    );
  if (!canvases) return <Spinner label="Opening your posters" />;
  if (!active)
    return (
      <EmptyState
        title="No posters yet"
        description="Your first poster is being prepared. It appears here as soon as it syncs."
      />
    );

  const selectCanvas = (id: string) => {
    setActiveId(id);
    setSelectedShapeId(null);
    setActiveLayerId(null);
    setPreviewCheckpointId(null);
  };

  return (
    <VStack gap={4} padding={4} minHeight="100dvh">
      <HStack as="header" justify="between" vAlign="center" wrap="wrap" gap={3}>
        <HStack gap={3} vAlign="center" wrap="wrap">
          <Heading level={1} maxLines={1}>
            {active.title}
          </Heading>
          {role && <Badge variant="neutral" label={roleLabel(role)} />}
        </HStack>
        <HStack gap={2} vAlign="center" wrap="wrap">
          <CollaboratorAvatars canvasId={active.id} author={author} />
          {canAdmin && <InviteButton canvasId={active.id} />}
          {canvases.length > 1 && (
            <Selector
              label="Poster"
              isLabelHidden
              value={active.id}
              onChange={selectCanvas}
              options={canvases.map((canvas) => ({ value: canvas.id, label: canvas.title }))}
            />
          )}
          <Button
            label="Sign out"
            variant="ghost"
            clickAction={async () => {
              await authClient.signOut();
              window.location.assign("/");
            }}
          />
        </HStack>
      </HStack>
      {notice}
      <div className="studio-grid">
        <aside className="studio-layers" aria-label="Layers">
          <LayerPanel
            canvasId={active.id}
            canEdit={canEdit}
            activeLayerId={activeLayerId}
            onActiveLayerChange={setActiveLayerId}
          />
        </aside>
        <main className="studio-canvas">
          <CanvasSurface
            canvasId={active.id}
            width={active.width}
            height={active.height}
            canEdit={canEdit}
            author={author}
            displayName={displayName}
            selectedShapeId={selectedShapeId}
            onSelectShape={setSelectedShapeId}
            activeLayerId={activeLayerId}
            previewCheckpointId={previewCheckpointId}
            onExitPreview={() => setPreviewCheckpointId(null)}
          />
        </main>
        <aside className="studio-panel" aria-label="Poster details">
          <VStack gap={4}>
            <SegmentedControl
              label="Panel"
              value={panel}
              onChange={(value) => setPanel(value as SidePanel)}
              layout="fill"
            >
              <SegmentedControlItem value="design" label="Design" />
              <SegmentedControlItem value="assets" label="Assets" />
              <SegmentedControlItem value="history" label="History" />
            </SegmentedControl>
            {panel === "design" && (
              <Inspector canvasId={active.id} shapeId={selectedShapeId} canEdit={canEdit} />
            )}
            {panel === "assets" && (
              <AssetShelf
                canvasId={active.id}
                canEdit={canEdit}
                posterWidth={active.width}
                posterHeight={active.height}
                activeLayerId={activeLayerId}
                onPlaced={(shapeId) => {
                  setSelectedShapeId(shapeId);
                  setPanel("design");
                }}
              />
            )}
            {panel === "history" && (
              <CheckpointShelf
                canvasId={active.id}
                canAdmin={canAdmin && !previewCheckpointId}
                previewCheckpointId={previewCheckpointId}
                onPreview={(id) => {
                  setSelectedShapeId(null);
                  setPreviewCheckpointId(id);
                }}
              />
            )}
          </VStack>
        </aside>
      </div>
    </VStack>
  );
}

function roleLabel(role: "viewer" | "editor" | "admin") {
  return role === "admin" ? "Admin" : role === "editor" ? "Editor" : "Viewer";
}
