import type * as React from "react";

/** What can be dropped on a folder: files from the computer, or an item from this app. */
export type DropPayload =
  | { kind: "files"; files: File[] }
  | { kind: "file" | "folder"; id: string };

const ITEM_TYPE = "application/x-epic-drop-item";

export function setDraggedItem(event: React.DragEvent, item: { kind: "file" | "folder"; id: string }) {
  event.dataTransfer.setData(ITEM_TYPE, JSON.stringify(item));
  event.dataTransfer.effectAllowed = "move";
}

export function isDroppable(event: React.DragEvent): boolean {
  const types = event.dataTransfer.types;
  return types.includes(ITEM_TYPE) || types.includes("Files");
}

export function readDrop(event: React.DragEvent): DropPayload | undefined {
  const item = event.dataTransfer.getData(ITEM_TYPE);
  if (item) return JSON.parse(item) as DropPayload;
  const files = [...event.dataTransfer.files];
  return files.length > 0 ? { kind: "files", files } : undefined;
}
