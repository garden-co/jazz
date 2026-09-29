import * as React from "react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import type { Folder, FolderIndex } from "../folders.js";

interface MoveDialogProps {
  item: { kind: "file" | "folder"; id: string; name: string; folderId?: string } | undefined;
  index: FolderIndex;
  /** `null` moves a folder to the top level. */
  onMove: (targetFolderId: string | null) => void;
  onClose: () => void;
}

const TOP_LEVEL = "top-level";

export function MoveDialog({ item, index, onMove, onClose }: MoveDialogProps) {
  const [target, setTarget] = React.useState<string>();
  const current = item?.kind === "folder" ? index.byId.get(item.id)?.parent_id : item?.folderId;
  const targets: Folder[] = item
    ? index
        .moveTargets(item.kind === "folder" ? item.id : undefined)
        .filter((folder) => folder.id !== current)
    : [];
  React.useEffect(() => setTarget(undefined), [item?.id]);
  return (
    <Dialog isOpen={item !== undefined} onOpenChange={(open) => !open && onClose()} width={440} purpose="form">
      <VStack gap={4}>
        <DialogHeader title={`Move ${item?.name ?? ""}`} onOpenChange={(open) => !open && onClose()} />
        <Selector
          label="Destination folder"
          placeholder="Choose a folder"
          hasSearch={targets.length > 8}
          emptyText="No other folders you can edit"
          options={[
            ...(item?.kind === "folder" && current ? [{ value: TOP_LEVEL, label: "Top level" }] : []),
            ...targets.map((folder) => ({ value: folder.id, label: index.label(folder) })),
          ]}
          value={target}
          onChange={setTarget}
        />
        <HStack gap={2} hAlign="end">
          <Button label="Cancel" variant="secondary" onClick={onClose} />
          <Button
            label="Move"
            variant="primary"
            isDisabled={!target}
            onClick={() => {
              if (!target) return;
              onMove(target === TOP_LEVEL ? null : target);
              onClose();
            }}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
