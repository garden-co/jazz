"use client";

import { useState } from "react";
import { useDb } from "jazz-tools/react";
import {
  Button,
  Item,
  List,
  MoreMenu,
  SideNav,
  SideNavHeading,
  SideNavItem,
  SideNavSection,
  type DropdownMenuOption,
} from "@astryxdesign/core";
import { app, type Member, type Page, type Workspace } from "@/schema";
import { PAGE_TREE_MAX_DEPTH } from "@/src/lib/limits";
import { createPage } from "@/src/lib/page-actions";
import { MovePageDialog } from "./MovePageDialog";
import { DeletePageDialog } from "./DeletePageDialog";
import { WorkspaceDialog } from "./WorkspaceDialog";
import { useCan } from "./use-can";
import { pageAdviceKey, ROLE_LABELS, useWorkspace } from "./workspace-context";

/** Workspace switcher and the nested page tree. */
export function Sidebar({
  workspaces,
  memberships,
  onSwitchWorkspace,
}: {
  workspaces: Workspace[];
  memberships: Member[];
  onSwitchWorkspace: (workspaceId: string) => void;
}) {
  const db = useDb();
  const { workspace, role, tree, openPage } = useWorkspace();
  const [dialog, setDialog] = useState<
    { kind: "move" | "delete"; pageId: string } | { kind: "workspace" } | null
  >(null);
  const canCreateTopLevel = useCan(
    (db) =>
      db.canInsert(app.pages, {
        workspaceId: workspace.id,
        parentId: null,
        title: "",
        kind: "doc",
      }),
    workspace.id,
  );
  const canManageBand = useCan(
    (db) => db.canUpdate(app.workspaces, workspace.id, { name: workspace.name }),
    workspace.id,
  );
  const roleOf = (workspaceId: string) =>
    memberships.find((member) => member.workspaceId === workspaceId)?.role;

  const addPage = (parentId: string | null) => {
    const page = createPage(db, { workspaceId: workspace.id, parentId, title: "" });
    openPage(page.id);
  };

  return (
    <>
      <SideNav
        header={
          <SideNavHeading
            heading={workspace.name}
            subheading={role ? ROLE_LABELS[role] : undefined}
            menu={
              <List density="compact" header="Your bands">
                {workspaces.map((candidate) => (
                  <Item
                    key={candidate.id}
                    label={candidate.name}
                    description={ROLE_LABELS[roleOf(candidate.id) ?? "guest"]}
                    isSelected={candidate.id === workspace.id}
                    onClick={() => onSwitchWorkspace(candidate.id)}
                  />
                ))}
                {canManageBand && (
                  <Item label="Band settings" onClick={() => setDialog({ kind: "workspace" })} />
                )}
              </List>
            }
          />
        }
        topContent={
          canCreateTopLevel ? (
            <Button label="New page" variant="ghost" width="100%" onClick={() => addPage(null)} />
          ) : undefined
        }
      >
        <SideNavSection title={role === "guest" ? "Shared with you" : "Pages"}>
          {tree.roots
            .filter((page) => page.kind !== "issue")
            .map((page) => (
              <PageNavItem
                key={page.id}
                page={page}
                onAddChild={addPage}
                onMove={(pageId) => setDialog({ kind: "move", pageId })}
                onDelete={(pageId) => setDialog({ kind: "delete", pageId })}
              />
            ))}
        </SideNavSection>
      </SideNav>
      {dialog?.kind === "move" && (
        <MovePageDialog pageId={dialog.pageId} onClose={() => setDialog(null)} />
      )}
      {dialog?.kind === "delete" && (
        <DeletePageDialog pageId={dialog.pageId} onClose={() => setDialog(null)} />
      )}
      {dialog?.kind === "workspace" && <WorkspaceDialog onClose={() => setDialog(null)} />}
    </>
  );
}

function PageNavItem({
  page,
  onAddChild,
  onMove,
  onDelete,
}: {
  page: Page;
  onAddChild: (parentId: string) => void;
  onMove: (pageId: string) => void;
  onDelete: (pageId: string) => void;
}) {
  const { tree, selectedPageId, openPage } = useWorkspace();
  const adviceKey = pageAdviceKey(tree, page.id);
  const canEdit = useCan(
    (db) => db.canUpdate(app.pages, page.id, { title: page.title }),
    adviceKey,
  );
  // Deleting and moving both need edit access from above the page.
  const canRestructure = useCan((db) => db.canDelete(app.pages, page.id), adviceKey);
  // Issues live in their database view, not in the sidebar.
  const children = page.kind === "issues" ? [] : tree.children(page.id);
  const canNest = tree.depth(page.id) + 1 < PAGE_TREE_MAX_DEPTH;
  const selectedIsInside =
    !!selectedPageId && tree.ancestors(selectedPageId).some((ancestor) => ancestor.id === page.id);

  const items: DropdownMenuOption[] = [];
  if (canEdit && canNest && page.kind === "doc")
    items.push({ label: "Add subpage", onClick: () => onAddChild(page.id) });
  if (canRestructure) {
    items.push({ label: "Move to…", onClick: () => onMove(page.id) });
    items.push({ type: "divider" });
    items.push({ label: "Delete", variant: "destructive", onClick: () => onDelete(page.id) });
  }

  return (
    <SideNavItem
      label={page.title || "Untitled"}
      isSelected={page.id === selectedPageId}
      onClick={() => openPage(page.id)}
      collapsible={
        children.length > 0
          ? { defaultIsCollapsed: !selectedIsInside && tree.depth(page.id) > 0 }
          : false
      }
      actions={
        items.length > 0 ? (
          <MoreMenu
            label={`Actions for ${page.title || "Untitled"}`}
            size="sm"
            items={items}
            alignment="end"
          />
        ) : undefined
      }
    >
      {children.length > 0
        ? children.map((child) => (
            <PageNavItem
              key={child.id}
              page={child}
              onAddChild={onAddChild}
              onMove={onMove}
              onDelete={onDelete}
            />
          ))
        : undefined}
    </SideNavItem>
  );
}
