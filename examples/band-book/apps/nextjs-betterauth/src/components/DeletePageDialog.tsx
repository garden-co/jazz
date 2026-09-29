"use client";

import { useDb } from "jazz-tools/react";
import { Button, Dialog, Heading, HStack, Text, VStack } from "@astryxdesign/core";
import { deletePageTree } from "@/src/lib/page-actions";
import { useWorkspace } from "./workspace-context";

export function DeletePageDialog({ pageId, onClose }: { pageId: string; onClose: () => void }) {
  const db = useDb();
  const { tree, selectedPageId, openPage } = useWorkspace();
  const page = tree.byId.get(pageId);
  if (!page) return null;
  const count = tree.descendantIds(pageId).size;
  return (
    <Dialog isOpen onOpenChange={(open) => !open && onClose()} padding={4} width={440}>
      <VStack gap={4}>
        <VStack gap={1}>
          <Heading level={2}>Delete “{page.title || "Untitled"}”?</Heading>
          <Text type="supporting">
            {count > 0
              ? `This also deletes ${count} page${count === 1 ? "" : "s"} inside it, for everyone in the band.`
              : "This deletes the page for everyone in the band."}
          </Text>
        </VStack>
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onClose} />
          <Button
            label="Delete"
            variant="destructive"
            clickAction={async () => {
              const leaving =
                selectedPageId === pageId ||
                (!!selectedPageId && tree.ancestors(selectedPageId).some((a) => a.id === pageId));
              await deletePageTree(db, tree, pageId);
              if (leaving) openPage(page.parentId ?? null);
              onClose();
            }}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
