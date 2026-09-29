import * as React from "react";
import { useDb } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { app } from "../../schema.js";
import type { FolderIndex } from "../folders.js";
import { offered, useAdvice, type Advice } from "../use-advice.js";

export interface MoveItem {
  kind: "file" | "folder";
  id: string;
  name: string;
  folderId?: string;
}

interface MoveDialogProps {
  item: MoveItem | undefined;
  index: FolderIndex;
  revision: string;
  /** `null` moves a folder to the top level. */
  onMove: (targetFolderId: string | null) => void;
  onClose: () => void;
}

const TOP_LEVEL = "top-level";

/** Asks Jazz whether `item` may move into `target` (`null` is the top level). */
export function checkMove(
  db: ReturnType<typeof useDb>,
  item: { kind: "file" | "folder"; id: string },
  target: string | null,
): Promise<Advice> {
  if (item.kind === "folder") return db.canUpdate(app.folders, item.id, { parent_id: target });
  return target === null
    ? Promise.resolve("denied")
    : db.canUpdate(app.files, item.id, { folder_id: target });
}

export function MoveDialog({ item, index, revision, onMove, onClose }: MoveDialogProps) {
  const db = useDb();
  const [target, setTarget] = React.useState<string>();
  const current = item?.kind === "folder" ? index.byId.get(item.id)?.parent_id : item?.folderId;
  const candidates = item
    ? index
        .moveCandidates(item.kind === "folder" ? item.id : undefined)
        .filter((folder) => folder.id !== current)
    : [];

  const checks: Record<string, () => Promise<Advice>> = {};
  if (item) {
    for (const folder of candidates) checks[folder.id] = () => checkMove(db, item, folder.id);
    if (item.kind === "folder" && current) checks[TOP_LEVEL] = () => checkMove(db, item, null);
  }
  const advice = useAdvice(checks, `${revision}:${item?.id ?? ""}`);
  const isChecking = Object.keys(checks).some((key) => advice[key] === undefined);

  React.useEffect(() => setTarget(undefined), [item?.id]);
  return (
    <Dialog
      isOpen={item !== undefined}
      onOpenChange={(open) => !open && onClose()}
      width={440}
      purpose="form"
    >
      <VStack gap={4}>
        <DialogHeader
          title={`Move ${item?.name ?? ""}`}
          onOpenChange={(open) => !open && onClose()}
        />
        <Selector
          label="Destination folder"
          placeholder={isChecking ? "Checking where it can go" : "Choose a folder"}
          hasSearch={candidates.length > 8}
          emptyText={
            item?.kind === "folder"
              ? "Only the folder's owner can move it, into folders they can edit"
              : "No other folders you can edit"
          }
          options={[
            ...(offered(advice[TOP_LEVEL]) ? [{ value: TOP_LEVEL, label: "Top level" }] : []),
            ...candidates
              .filter((folder) => offered(advice[folder.id]))
              .map((folder) => ({ value: folder.id, label: index.label(folder) })),
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
