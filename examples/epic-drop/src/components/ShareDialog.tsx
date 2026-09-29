import * as React from "react";
import { useAll, useDb } from "jazz-tools/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Divider } from "@astryxdesign/core/Divider";
import { useClipboard } from "@astryxdesign/core/hooks";
import { List, ListItem } from "@astryxdesign/core/List";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { Selector } from "@astryxdesign/core/Selector";
import { HStack, StackItem, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app, type FolderRole } from "../../schema.js";
import { createInvite, inviteLink } from "../sharing.js";
import { useNames } from "../profiles.js";

const ROLE_LABEL: Record<FolderRole, string> = { viewer: "Can view", editor: "Can edit" };

interface ShareDialogProps {
  folder: { id: string; name: string } | undefined;
  userId: string | undefined;
  onClose: () => void;
}

/** Owners invite people by link and manage who has access to a folder. */
export function ShareDialog({ folder, userId, onClose }: ShareDialogProps) {
  const db = useDb();
  const folderId = folder?.id;
  const { data: members = [] } = useAll(
    folderId ? app.folderMembers.where({ folder_id: folderId }) : undefined,
  );
  const { data: invites = [] } = useAll(
    folderId ? app.folderInvites.where({ folder_id: folderId }) : undefined,
  );
  const nameOf = useNames(
    members.map((member) => member.user_id),
    userId,
  );
  const [role, setRole] = React.useState<FolderRole>("viewer");
  const [link, setLink] = React.useState<string>();
  const { copy, isCopied } = useClipboard();
  React.useEffect(() => setLink(undefined), [folderId]);

  return (
    <Dialog
      isOpen={folder !== undefined}
      onOpenChange={(open) => !open && onClose()}
      width={520}
      maxHeight="90dvh"
    >
      <VStack gap={5}>
        <DialogHeader
          title={`Share ${folder?.name ?? ""}`}
          onOpenChange={(open) => !open && onClose()}
        />
        <VStack gap={3}>
          <Heading level={3}>Invite with a link</Heading>
          <Text color="secondary">
            Anyone with the link can join with this access. Subfolders and files are shared too.
          </Text>
          <HStack gap={2} vAlign="center" wrap="wrap">
            <SegmentedControl
              label="Access"
              value={role}
              onChange={(value) => setRole(value as FolderRole)}
            >
              <SegmentedControlItem value="viewer" label={ROLE_LABEL.viewer} />
              <SegmentedControlItem value="editor" label={ROLE_LABEL.editor} />
            </SegmentedControl>
            <Button
              label="Create link"
              variant="primary"
              onClick={() => {
                if (!folderId) return;
                const next = inviteLink(createInvite(db, folderId, role));
                setLink(next);
                void copy(next);
              }}
            />
          </HStack>
          {link && (
            <HStack gap={2} vAlign="end">
              <StackItem size="fill">
                <TextInput label="Invite link" value={link} isReadOnly />
              </StackItem>
              <Button label={isCopied ? "Copied" : "Copy"} onClick={() => void copy(link)} />
            </HStack>
          )}
        </VStack>
        <Divider />
        <VStack gap={2}>
          <Heading level={3}>People with access</Heading>
          <List density="compact" hasDividers>
            <ListItem
              startContent={<Avatar name="You" size="sm" tooltip={false} />}
              label="You"
              endContent={<Badge label="Owner" />}
            />
            {members.map((member) => (
              <ListItem
                key={member.id}
                startContent={<Avatar name={nameOf(member.user_id)} size="sm" tooltip={false} />}
                label={nameOf(member.user_id)}
                endContent={
                  <HStack gap={1} vAlign="center">
                    <Selector
                      label={`Access for ${nameOf(member.user_id)}`}
                      isLabelHidden
                      size="sm"
                      variant="ghost"
                      value={member.role}
                      options={[
                        { value: "viewer", label: ROLE_LABEL.viewer },
                        { value: "editor", label: ROLE_LABEL.editor },
                      ]}
                      onChange={(value) =>
                        db.update(app.folderMembers, member.id, { role: value as FolderRole })
                      }
                    />
                    <Button
                      label="Remove"
                      variant="ghost"
                      size="sm"
                      onClick={() => db.delete(app.folderMembers, member.id)}
                    />
                  </HStack>
                }
              />
            ))}
          </List>
        </VStack>
        {invites.length > 0 && (
          <VStack gap={2}>
            <Heading level={3}>Active links</Heading>
            <Text color="secondary">
              Revoking a link stops new people joining with it. People who joined keep access until
              you remove them.
            </Text>
            <List density="compact" hasDividers>
              {invites.map((invite) => (
                <ListItem
                  key={invite.id}
                  label={`${ROLE_LABEL[invite.role]} link`}
                  description={`…${invite.code.slice(-6)}`}
                  endContent={
                    <Button
                      label="Revoke"
                      variant="ghost"
                      size="sm"
                      onClick={() => db.delete(app.folderInvites, invite.id)}
                    />
                  }
                />
              ))}
            </List>
          </VStack>
        )}
      </VStack>
    </Dialog>
  );
}
