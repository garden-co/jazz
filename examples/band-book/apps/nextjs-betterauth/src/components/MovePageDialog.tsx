"use client";

import { useEffect, useState } from "react";
import { useDb } from "jazz-tools/react";
import { Button, Dialog, Heading, HStack, Selector, Text, VStack } from "@astryxdesign/core";
import { app } from "@/schema";
import { PAGE_TREE_MAX_DEPTH } from "@/src/lib/limits";
import { moveTargets } from "@/src/lib/page-actions";
import { useWorkspace } from "./workspace-context";

const TOP_LEVEL = "top-level";

/** Pick a new parent. The policy checks edit access to the destination. */
export function MovePageDialog({ pageId, onClose }: { pageId: string; onClose: () => void }) {
  const db = useDb();
  const { tree, pages, accessVersion } = useWorkspace();
  const page = tree.byId.get(pageId);
  const [target, setTarget] = useState(page?.parentId ?? TOP_LEVEL);
  // Destinations the policy would accept, from core's permission advice.
  const [allowed, setAllowed] = useState<ReadonlySet<string> | null>(null);
  const candidates = page
    ? moveTargets(tree, pages, pageId).filter((candidate) => {
        const subtreeHeight = Math.max(
          0,
          ...[...tree.descendantIds(pageId)].map((id) => tree.depth(id) - tree.depth(pageId)),
        );
        return tree.depth(candidate.id) + 1 + subtreeHeight < PAGE_TREE_MAX_DEPTH;
      })
    : [];
  const candidateKey = candidates.map((candidate) => candidate.id).join(",");

  useEffect(() => {
    let current = true;
    const parents = [TOP_LEVEL, ...candidateKey.split(",").filter(Boolean)];
    void Promise.all(
      parents.map(async (parent) => {
        const parentId = parent === TOP_LEVEL ? null : parent;
        const advice = await db.canUpdate(app.pages, pageId, { parentId }).catch(() => "denied");
        return advice === "denied" ? null : parent;
      }),
    ).then((results) => {
      if (current) setAllowed(new Set(results.filter((parent) => parent !== null)));
    });
    return () => {
      current = false;
    };
  }, [db, pageId, candidateKey, accessVersion]);

  if (!page) return null;
  const options = candidates
    .filter((candidate) => allowed?.has(candidate.id))
    .map((candidate) => ({
      value: candidate.id,
      label: [...tree.ancestors(candidate.id), candidate]
        .map((node) => node.title || "Untitled")
        .join(" / "),
    }));
  const canTopLevel = allowed?.has(TOP_LEVEL) ?? false;
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
