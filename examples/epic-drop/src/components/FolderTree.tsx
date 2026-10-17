import * as React from "react";
import { TreeList } from "@astryxdesign/core/TreeList";
import type { TreeListItemData } from "@astryxdesign/core/TreeList";
import type { Folder, FolderIndex } from "../folders.js";
import { DropTarget } from "./DropTarget.js";
import type { DropPayload } from "../drag.js";

interface FolderTreeProps {
  label: string;
  roots: readonly Folder[];
  index: FolderIndex;
  selectedId: string | undefined;
  canDropOn: (folderId: string) => boolean;
  onSelect: (folderId: string) => void;
  onDrop: (folderId: string, payload: DropPayload) => void;
}

/** A folder tree whose rows are drop targets for dragged files and folders. */
export function FolderTree({
  label,
  roots,
  index,
  selectedId,
  canDropOn,
  onSelect,
  onDrop,
}: FolderTreeProps) {
  const openPath = new Set(index.path(selectedId).map((folder) => folder.id));
  const toItem = (folder: Folder): TreeListItemData => {
    const children = index.children.get(folder.id);
    return {
      id: folder.id,
      label: (
        <DropTarget
          isDisabled={!canDropOn(folder.id)}
          onDrop={(payload) => onDrop(folder.id, payload)}
        >
          {folder.name}
        </DropTarget>
      ),
      isSelected: folder.id === selectedId,
      isExpanded: openPath.has(folder.id),
      onClick: () => onSelect(folder.id),
      children: children?.map(toItem),
    };
  };
  return <TreeList aria-label={label} density="compact" items={roots.map(toItem)} />;
}
