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
import { app, type WorkspaceRole } from "@/schema";
import { createInviteToken } from "@/src/lib/invites";
import { InviteLink } from "./ShareDialog";
import { ROLE_LABELS, useWorkspace } from "./workspace-context";

const BAND_ROLES: WorkspaceRole[] = ["owner", "member", "viewer"];

/** Owners rename the band, change roles and invite people to the whole band. */
export function WorkspaceDialog({ onClose }: { onClose: () => void }) {
  const db = useDb();
  const { workspace, members, me } = useWorkspace();
  const [name, setName] = useState(workspace.name);
  const [inviteRole, setInviteRole] = useState<"member" | "viewer">("member");
  const { data: invites } = useAll(app.invites.where({ workspaceId: workspace.id, pageId: null }));

  return (
    <Dialog isOpen onOpenChange={(open) => !open && onClose()} padding={4} width={560}>
      <VStack gap={4}>
        <Heading level={2}>Band settings</Heading>
        <HStack gap={2} align="end">
          <TextInput label="Band name" value={name} onChange={setName} width="100%" />
          <Button
            label="Rename"
            isDisabled={!name.trim() || name === workspace.name}
            onClick={() => db.update(app.workspaces, workspace.id, { name: name.trim() })}
          />
        </HStack>

        <List header="People" density="compact">
          {members.map((member) => (
            <Item
              key={member.id}
              startContent={<Avatar name={member.displayName} size="sm" tooltip={false} />}
              label={member.account === me ? `${member.displayName} (you)` : member.displayName}
              description={
                member.role === "guest" ? "Guest with access to shared pages" : undefined
              }
              endContent={
                member.role === "guest" || member.account === me ? (
                  <Text type="supporting">{ROLE_LABELS[member.role]}</Text>
                ) : (
                  <HStack gap={1} align="center">
                    <Selector
                      label={`Role for ${member.displayName}`}
                      isLabelHidden
                      size="sm"
                      variant="ghost"
                      value={member.role}
                      options={BAND_ROLES.map((role) => ({
                        value: role,
                        label: ROLE_LABELS[role],
                      }))}
                      onChange={(role) =>
                        db.update(app.members, member.id, { role: role as WorkspaceRole })
                      }
                    />
                    <Button
                      label="Remove"
                      size="sm"
                      variant="ghost"
                      onClick={() => db.delete(app.members, member.id)}
                    />
                  </HStack>
                )
              }
            />
          ))}
        </List>

        <Divider />

        <VStack gap={3}>
          <VStack gap={1}>
            <Heading level={3}>Invite to the band</Heading>
            <Text type="supporting">
              Band members can edit and share every page. Crew can read everything.
            </Text>
          </VStack>
          <HStack gap={2} align="end" wrap="wrap">
            <Selector
              label="Role"
              value={inviteRole}
              options={[
                { value: "member", label: ROLE_LABELS.member },
                { value: "viewer", label: ROLE_LABELS.viewer },
              ]}
              onChange={(value) => setInviteRole(value as "member" | "viewer")}
              width={200}
            />
            <Button
              label="Create link"
              onClick={() =>
                db.insert(app.invites, {
                  workspaceId: workspace.id,
                  pageId: null,
                  role: inviteRole,
                  token: createInviteToken(),
                  label: `${ROLE_LABELS[inviteRole]}: ${workspace.name}`,
                })
              }
            />
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
