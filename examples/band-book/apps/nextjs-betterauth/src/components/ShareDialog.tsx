"use client";

import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import {
  Avatar,
  Button,
  Dialog,
  Divider,
  Heading,
  HStack,
  Item,
  List,
  Selector,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { app, type GrantRole, type Invite } from "@/schema";
import { createInviteToken } from "@/src/lib/invites";
import { GRANT_LABELS, memberName, ROLE_LABELS, useWorkspace } from "./workspace-context";

const GRANT_OPTIONS = (Object.keys(GRANT_LABELS) as GrantRole[]).map((role) => ({
  value: role,
  label: GRANT_LABELS[role],
}));

/**
 * Who can reach a page, and invite links for more people. A grant covers the
 * page and every page below it, which is how a guest collaborator gets one
 * song and nothing else.
 */
export function ShareDialog({ pageId, onClose }: { pageId: string; onClose: () => void }) {
  const db = useDb();
  const { workspace, tree, members } = useWorkspace();
  const page = tree.byId.get(pageId)!;
  const chain = [...tree.ancestors(pageId), page];
  const { data: grants } = useAll(
    app.pageGrants.where({ pageId: { in: chain.map((node) => node.id) } }),
  );
  const { data: invites } = useAll(app.invites.where({ pageId }));
  const [role, setRole] = useState<GrantRole>("editor");
  const bandPeople = members.filter((member) => member.role !== "guest");

  const createLink = () =>
    db.insert(app.invites, {
      workspaceId: workspace.id,
      pageId,
      role,
      token: createInviteToken(),
      label: `${GRANT_LABELS[role]}: ${page.title || "Untitled"}`,
    });

  return (
    <Dialog isOpen onOpenChange={(open) => !open && onClose()} padding={4} width={560}>
      <VStack gap={4}>
        <VStack gap={1}>
          <Heading level={2}>Share “{page.title || "Untitled"}”</Heading>
          <Text type="supporting">Access to a page includes every page inside it.</Text>
        </VStack>

        <List header="People with access" density="compact">
          <Item
            label={`Everyone in ${workspace.name}`}
            description={`${bandPeople.length} ${bandPeople.length === 1 ? "person" : "people"} with a band role`}
          />
          {(grants ?? []).map((grant) => {
            const inherited = grant.pageId !== pageId;
            const name = memberName(members, grant.account);
            return (
              <Item
                key={grant.id}
                startContent={<Avatar name={name} size="sm" tooltip={false} />}
                label={name}
                description={
                  inherited
                    ? `${GRANT_LABELS[grant.role]} via ${tree.byId.get(grant.pageId)?.title || "a parent page"}`
                    : GRANT_LABELS[grant.role]
                }
                endContent={
                  inherited ? undefined : (
                    <HStack gap={1} align="center">
                      <Selector
                        label={`Access for ${name}`}
                        isLabelHidden
                        size="sm"
                        variant="ghost"
                        value={grant.role}
                        options={GRANT_OPTIONS}
                        onChange={(value) =>
                          db.update(app.pageGrants, grant.id, { role: value as GrantRole })
                        }
                      />
                      <Button
                        label="Remove"
                        size="sm"
                        variant="ghost"
                        onClick={() => db.delete(app.pageGrants, grant.id)}
                      />
                    </HStack>
                  )
                }
              />
            );
          })}
        </List>

        <Divider />

        <VStack gap={3}>
          <VStack gap={1}>
            <Heading level={3}>Invite link</Heading>
            <Text type="supporting">
              Anyone who opens the link and signs in joins as a guest with access to this page only.
            </Text>
          </VStack>
          <HStack gap={2} align="end" wrap="wrap">
            <Selector
              label="Access"
              size="md"
              value={role}
              options={GRANT_OPTIONS}
              onChange={(value) => setRole(value as GrantRole)}
              width={180}
            />
            <Button label="Create link" onClick={createLink} />
          </HStack>
          {(invites ?? []).map((invite) => (
            <InviteLink key={invite.id} invite={invite} />
          ))}
        </VStack>

        <HStack justify="end">
          <Button label="Done" variant="primary" onClick={onClose} />
        </HStack>
      </VStack>
    </Dialog>
  );
}

export function InviteLink({ invite }: { invite: Invite }) {
  const db = useDb();
  const [copied, setCopied] = useState(false);
  const url =
    typeof window === "undefined" ? "" : `${window.location.origin}/invite/${invite.token}`;
  const roleLabel = invite.pageId
    ? GRANT_LABELS[invite.role === "editor" ? "editor" : "viewer"]
    : ROLE_LABELS[invite.role === "member" ? "member" : "viewer"];
  return (
    <HStack gap={2} align="end" wrap="wrap">
      <TextInput label={roleLabel} value={url} isReadOnly width="100%" />
      <HStack gap={1}>
        <Button
          label={copied ? "Copied" : "Copy"}
          size="sm"
          onClick={() =>
            void navigator.clipboard.writeText(url).then(() => {
              setCopied(true);
              setTimeout(() => setCopied(false), 2000);
            })
          }
        />
        <Button
          label="Revoke"
          size="sm"
          variant="ghost"
          onClick={() => db.delete(app.invites, invite.id)}
        />
      </HStack>
    </HStack>
  );
}
