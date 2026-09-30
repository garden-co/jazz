"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { AlertDialog } from "@astryxdesign/core";
import { deletePageTree } from "@/src/lib/page-actions";
import { useWorkspace } from "./workspace-context";

export function DeletePageDialog({ pageId, onClose }: { pageId: string; onClose: () => void }) {
  const db = useDb();
  const { tree, selectedPageId, openPage } = useWorkspace();
  const [deleting, setDeleting] = useState(false);
  const page = tree.byId.get(pageId);
  if (!page) return null;
  const count = tree.descendantIds(pageId).size;

  const confirm = async () => {
    const leaving =
      selectedPageId === pageId ||
      (!!selectedPageId && tree.ancestors(selectedPageId).some((a) => a.id === pageId));
    setDeleting(true);
    try {
      await deletePageTree(db, tree, pageId);
      if (leaving) openPage(page.parentId ?? null);
    } finally {
      setDeleting(false);
      onClose();
    }
  };

  return (
    <AlertDialog
      isOpen
      onOpenChange={(open) => !open && !deleting && onClose()}
      title={`Delete “${page.title || "Untitled"}”?`}
      description={
        count > 0
          ? `This also deletes ${count} page${count === 1 ? "" : "s"} inside it, for everyone in the band. Share links to these pages stop working.`
          : "This deletes the page for everyone in the band. Share links to it stop working."
      }
      actionLabel="Delete"
      isActionLoading={deleting}
      onAction={() => void confirm()}
      width={440}
    />
  );
}
