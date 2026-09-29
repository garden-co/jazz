import * as React from "react";
import { HStack } from "@astryxdesign/core/Stack";
import { Icon } from "@astryxdesign/core/Icon";
import { Link } from "@astryxdesign/core/Link";
import { MoreMenu } from "@astryxdesign/core/MoreMenu";
import type { DropdownMenuOption } from "@astryxdesign/core/DropdownMenu";
import { Text } from "@astryxdesign/core/Text";
import { Timestamp } from "@astryxdesign/core/Timestamp";
import { VisuallyHidden } from "@astryxdesign/core/VisuallyHidden";
import {
  Table,
  pixel,
  proportional,
  useTableSortable,
  useTableSortableState,
  type TableColumn,
  type TablePlugin,
} from "@astryxdesign/core/Table";
import { useMediaQuery } from "@astryxdesign/core/hooks";
import { formatBytes } from "../large-values.js";
import { setDraggedItem, type DropPayload } from "../drag.js";
import { DropTarget } from "./DropTarget.js";

export interface Entry extends Record<string, unknown> {
  id: string;
  kind: "folder" | "file";
  name: string;
  type: string;
  size: number;
  modified: Date | null;
  owner: string;
}

export type EntryAction = "open" | "download" | "rename" | "move" | "share" | "delete";

interface FileTableProps {
  entries: Entry[];
  canEdit: boolean;
  canShare: (entry: Entry) => boolean;
  onAction: (entry: Entry, action: EntryAction) => void;
  onDropOnFolder: (folderId: string, payload: DropPayload) => void;
}

type SortKey = "name" | "type" | "size" | "modified" | "owner";

const comparators = {
  size: (a: Entry, b: Entry) => a.size - b.size,
  modified: (a: Entry, b: Entry) => (a.modified?.getTime() ?? 0) - (b.modified?.getTime() ?? 0),
};

/** Folders first, then files; each group sorted by the chosen column. */
export function FileTable({
  entries,
  canEdit,
  canShare,
  onAction,
  onDropOnFolder,
}: FileTableProps) {
  const isNarrow = useMediaQuery("(max-width: 720px)");
  const { sortConfig, applySort } = useTableSortableState<Entry, SortKey>({
    data: entries,
    defaultSort: [{ sortKey: "name", direction: "ascending" }],
    comparators,
  });
  const sortPlugin = useTableSortable<Entry, SortKey>(sortConfig);
  const data = React.useMemo(
    () => [
      ...applySort(entries.filter((entry) => entry.kind === "folder")),
      ...applySort(entries.filter((entry) => entry.kind === "file")),
    ],
    [applySort, entries],
  );

  const dragPlugin = React.useMemo<TablePlugin<Entry>>(
    () => ({
      transformBodyRow: (props, item) =>
        canEdit
          ? {
              ...props,
              htmlProps: {
                ...props.htmlProps,
                draggable: true,
                onDragStart: (event) => setDraggedItem(event, { kind: item.kind, id: item.id }),
              },
            }
          : props,
    }),
    [canEdit],
  );

  const columns: TableColumn<Entry>[] = [
    {
      key: "name",
      header: "Name",
      sortable: true,
      width: proportional(3),
      renderCell: (entry) => {
        const name = (
          <HStack gap={2} vAlign="center">
            <Link isStandalone onClick={() => onAction(entry, "open")} maxLines={1}>
              {entry.name}
            </Link>
            {entry.kind === "folder" && (
              <Icon icon="chevronRight" size="sm" color="secondary" label="Folder" />
            )}
          </HStack>
        );
        return entry.kind === "folder" && canEdit ? (
          <DropTarget onDrop={(payload) => onDropOnFolder(entry.id, payload)}>{name}</DropTarget>
        ) : (
          name
        );
      },
    },
    ...(isNarrow
      ? []
      : ([
          { key: "type", header: "Type", sortable: true, width: proportional(1) },
        ] satisfies TableColumn<Entry>[])),
    {
      key: "size",
      header: "Size",
      sortable: true,
      align: "end",
      width: pixel(96),
      renderCell: (entry) =>
        entry.kind === "folder" ? null : <Text hasTabularNumbers>{formatBytes(entry.size)}</Text>,
    },
    ...(isNarrow
      ? []
      : ([
          {
            key: "modified",
            header: "Modified",
            sortable: true,
            width: pixel(152),
            renderCell: (entry) =>
              entry.modified ? <Timestamp value={entry.modified.getTime()} format="auto" /> : null,
          },
          { key: "owner", header: "Owner", sortable: true, width: proportional(1) },
        ] satisfies TableColumn<Entry>[])),
    {
      key: "actions",
      header: <VisuallyHidden>Actions</VisuallyHidden>,
      width: pixel(56),
      align: "end",
      renderCell: (entry) => (
        <MoreMenu
          label={`Actions for ${entry.name}`}
          size="sm"
          alignment="end"
          presentation="adaptive"
          items={menuItems(entry, canEdit, canShare(entry), (action) => onAction(entry, action))}
        />
      ),
    },
  ];

  return (
    <Table<Entry>
      data={data}
      columns={columns}
      idKey="id"
      density="compact"
      hasHover
      textOverflow="truncate"
      plugins={{ sort: sortPlugin, drag: dragPlugin }}
    />
  );
}

function menuItems(
  entry: Entry,
  canEdit: boolean,
  canShare: boolean,
  run: (action: EntryAction) => void,
): DropdownMenuOption[] {
  const items: DropdownMenuOption[] = [
    { label: entry.kind === "folder" ? "Open" : "Preview", onClick: () => run("open") },
  ];
  if (entry.kind === "file") items.push({ label: "Download", onClick: () => run("download") });
  if (canShare) items.push({ label: "Share", onClick: () => run("share") });
  if (canEdit) {
    items.push(
      { label: "Rename", onClick: () => run("rename") },
      { label: "Move", onClick: () => run("move") },
      { type: "divider" },
      { label: "Delete", variant: "destructive", onClick: () => run("delete") },
    );
  }
  return items;
}
