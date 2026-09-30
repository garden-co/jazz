"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Badge,
  BreadcrumbItem,
  Breadcrumbs,
  Button,
  EmptyState,
  HStack,
  Item,
  List,
  MoreMenu,
  VStack,
  type DropdownMenuOption,
} from "@astryxdesign/core";
import { app } from "@/schema";
import { PAGE_TREE_MAX_DEPTH } from "@/src/lib/limits";
import { createPage } from "@/src/lib/page-actions";
import { BlockEditor } from "./BlockEditor";
import { DeletePageDialog } from "./DeletePageDialog";
import { InlineText } from "./InlineText";
import { IssueProperties } from "./IssueProperties";
import { IssuesDatabase } from "./IssuesDatabase";
import { MovePageDialog } from "./MovePageDialog";
import { ShareDialog } from "./ShareDialog";
import { useCan } from "./use-can";
import { pageAdviceKey, useWorkspace } from "./workspace-context";

/** The selected page: breadcrumbs, title, actions and its body. */
export function PageScreen() {
  const { tree, selectedPageId } = useWorkspace();
  const page =
    (selectedPageId ? tree.byId.get(selectedPageId) : undefined) ??
    tree.roots.find((root) => root.kind !== "issue");
  if (!page)
    return (
      <VStack padding={6}>
        <EmptyState
          title="Nothing here yet"
          description="Pages you create or that are shared with you appear in the sidebar."
        />
      </VStack>
    );
  return <PageBody key={page.id} pageId={page.id} />;
}

function PageBody({ pageId }: { pageId: string }) {
  const db = useDb();
  const { me, workspace, tree, openPage } = useWorkspace();
  const page = tree.byId.get(pageId)!;
  const adviceKey = pageAdviceKey(tree, pageId);
  const canEdit = useCan((db) => db.canUpdate(app.pages, pageId, { title: page.title }), adviceKey);
  const editable = canEdit === true;
  // Deleting and moving both need edit access from above the page.
  const canRestructure = useCan((db) => db.canDelete(app.pages, pageId), adviceKey);
  const canShare = useCan(
    (db) =>
      db.canInsert(app.pageGrants, {
        workspaceId: workspace.id,
        pageId,
        account: me,
        role: "viewer",
      }),
    pageId,
  );
  const [dialog, setDialog] = useState<"share" | "move" | "delete" | null>(null);
  const ancestors = tree.ancestors(pageId);
  const subpages = page.kind === "issues" ? [] : tree.children(pageId);
  const canNest = editable && page.kind === "doc" && tree.depth(pageId) + 1 < PAGE_TREE_MAX_DEPTH;

  const menu: DropdownMenuOption[] = [];
  if (canNest)
    menu.push({
      label: "Add subpage",
      onClick: () =>
        openPage(createPage(db, { workspaceId: workspace.id, parentId: pageId, title: "" }).id),
    });
  if (canRestructure && page.kind !== "issue")
    menu.push({ label: "Move to…", onClick: () => setDialog("move") });
  if (canRestructure)
    menu.push({ label: "Delete", variant: "destructive", onClick: () => setDialog("delete") });

  return (
    <VStack gap={6} padding={6} paddingInline={4} width="100%" maxWidth={880}>
      <VStack gap={3}>
        <HStack gap={2} justify="between" align="center" wrap="wrap">
          <Breadcrumbs variant="supporting">
            <BreadcrumbItem onClick={() => openPage(null)}>{workspace.name}</BreadcrumbItem>
            {ancestors.map((ancestor) => (
              <BreadcrumbItem key={ancestor.id} onClick={() => openPage(ancestor.id)}>
                {ancestor.title || "Untitled"}
              </BreadcrumbItem>
            ))}
            <BreadcrumbItem isCurrent>{page.title || "Untitled"}</BreadcrumbItem>
          </Breadcrumbs>
          <HStack gap={2} align="center">
            {canEdit === false && <Badge label="View only" />}
            {canShare && <Button label="Share" size="sm" onClick={() => setDialog("share")} />}
            {menu.length > 0 && (
              <MoreMenu label="Page actions" size="sm" items={menu} alignment="end" />
            )}
          </HStack>
        </HStack>
        <InlineText
          variant="title"
          label="Page title"
          value={page.title}
          placeholder="Untitled"
          isReadOnly={!editable}
          hasAutoFocus={editable && page.title === ""}
          onChange={(title) => db.update(app.pages, pageId, { title })}
        />
      </VStack>

      {page.kind === "issue" && <IssueProperties pageId={pageId} editable={editable} />}
      {page.kind === "issues" ? (
        <IssuesDatabase databaseId={pageId} editable={editable} />
      ) : (
        <BlockEditor pageId={pageId} editable={editable} />
      )}

      {subpages.length > 0 && (
        <List header="Subpages" hasDividers density="compact">
          {subpages.map((child) => (
            <Item
              key={child.id}
              label={child.title || "Untitled"}
              description={`${tree.children(child.id).length || "No"} subpage${tree.children(child.id).length === 1 ? "" : "s"}`}
              onClick={() => openPage(child.id)}
            />
          ))}
        </List>
      )}

      {dialog === "share" && <ShareDialog pageId={pageId} onClose={() => setDialog(null)} />}
      {dialog === "move" && <MovePageDialog pageId={pageId} onClose={() => setDialog(null)} />}
      {dialog === "delete" && <DeletePageDialog pageId={pageId} onClose={() => setDialog(null)} />}
    </VStack>
  );
}
