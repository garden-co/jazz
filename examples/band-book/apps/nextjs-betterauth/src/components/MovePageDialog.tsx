"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import { Button, Dialog, Heading, HStack, Selector, Text, VStack } from "@astryxdesign/core";
import { app } from "@/schema";
import { PAGE_TREE_MAX_DEPTH } from "@/src/lib/limits";
import { moveTargets } from "@/src/lib/page-actions";
import { pageAccess } from "@/src/lib/tree";
import { useWorkspace } from "./workspace-context";

const TOP_LEVEL = "top-level";

/** Pick a new parent. The policy checks edit access to the destination. */
export function MovePageDialog({ pageId, onClose }: { pageId: string; onClose: () => void }) {
  const db = useDb();
  const { tree, pages, role, grants } = useWorkspace();
  const page = tree.byId.get(pageId);
  const [target, setTarget] = useState(page?.parentId ?? TOP_LEVEL);
  if (!page) return null;
  const subtreeHeight = Math.max(
    0,
    ...[...tree.descendantIds(pageId)].map((id) => tree.depth(id) - tree.depth(pageId)),
  );
  const options = moveTargets(tree, pages, pageId)
    .filter((candidate) => pageAccess(tree, candidate.id, role, grants) === "edit")
    .filter((candidate) => tree.depth(candidate.id) + 1 + subtreeHeight < PAGE_TREE_MAX_DEPTH)
    .map((candidate) => ({
      value: candidate.id,
      label: [...tree.ancestors(candidate.id), candidate]
        .map((node) => node.title || "Untitled")
        .join(" / "),
    }));
  const canTopLevel = role === "owner" || role === "member";
  return (
    <Dialog
      isOpen
      onOpenChange={(open) => !open && onClose()}
      purpose="form"
      padding={4}
      width={480}
    >
      <VStack gap={4}>
        <VStack gap={1}>
          <Heading level={2}>Move “{page.title || "Untitled"}”</Heading>
          <Text type="supporting">
            Subpages move with it. Access is inherited from the new parent.
          </Text>
        </VStack>
        <Selector
          label="New parent"
          value={target}
          onChange={setTarget}
          hasSearch
          options={[...(canTopLevel ? [{ value: TOP_LEVEL, label: "Top level" }] : []), ...options]}
        />
        <HStack gap={2} justify="end">
          <Button label="Cancel" variant="ghost" onClick={onClose} />
          <Button
            label="Move"
            variant="primary"
            onClick={() => {
              db.update(app.pages, pageId, { parentId: target === TOP_LEVEL ? null : target });
              onClose();
            }}
          />
        </HStack>
      </VStack>
    </Dialog>
  );
}
