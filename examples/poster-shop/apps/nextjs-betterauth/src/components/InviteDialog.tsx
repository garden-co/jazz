"use client";

import {
  Button,
  Dialog,
  DialogHeader,
  Heading,
  HStack,
  List,
  ListItem,
  SegmentedControl,
  SegmentedControlItem,
  Switch,
  Text,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { useAll, useDb } from "jazz-tools/react";
import { UserPlus } from "lucide-react";
import { useState } from "react";
import { app } from "@/schema";
import { inviteLinkFor } from "@/src/lib/account-enrollment";

type InviteRole = "editor" | "viewer";

/**
 * Admins issue, copy and revoke invite links; redeeming one happens
 * server-side. The token lives in the link's URL fragment only.
 */
export function InviteButton({ canvasId }: { canvasId: string }) {
  const db = useDb();
  const [open, setOpen] = useState(false);
  const [role, setRole] = useState<InviteRole>("editor");
  const [singleUse, setSingleUse] = useState(true);
  const [link, setLink] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  // Only admins can read invites (see permissions.ts), and only admins see
  // this button.
  const { data: invites = [] } = useAll(
    open
      ? app.canvasInvites
          .where({ canvasId })
          .select("id", "token", "role", "singleUse", "$createdAt")
          .orderBy("$createdAt", "desc")
      : undefined,
  );

  const createLink = () => {
    const token = crypto.randomUUID();
    db.insert(app.canvasInvites, { canvasId, token, role, singleUse });
    setLink(inviteLinkFor(window.location.origin, { canvasId, token }));
    setCopied(false);
  };

  const copy = async (value: string) => {
    await navigator.clipboard.writeText(value);
    setCopied(true);
  };

  return (
    <>
      <Button
        label="Invite"
        variant="secondary"
        icon={<UserPlus />}
        onClick={() => {
          setLink(null);
          setOpen(true);
        }}
      />
      <Dialog isOpen={open} onOpenChange={setOpen} width="30rem">
        <DialogHeader title="Invite collaborators" onOpenChange={setOpen} />
        <VStack gap={4} padding={4}>
          <Text color="secondary">
            Anyone who signs in with this link joins the poster with the role you choose.
          </Text>
          <SegmentedControl
            label="Role"
            value={role}
            onChange={(value) => {
              setRole(value as InviteRole);
              setLink(null);
            }}
            layout="fill"
          >
            <SegmentedControlItem value="editor" label="Can edit" />
            <SegmentedControlItem value="viewer" label="Can view" />
          </SegmentedControl>
          <Switch
            label="Single use"
            description="The link stops working once someone has joined with it."
            value={singleUse}
            onChange={(checked) => {
              setSingleUse(checked);
              setLink(null);
            }}
          />
          {link ? (
            <VStack gap={2}>
              <TextInput label="Invite link" value={link} isReadOnly />
              <Button
                label={copied ? "Link copied" : "Copy link"}
                variant="primary"
                clickAction={() => copy(link)}
              />
            </VStack>
          ) : (
            <Button label="Create invite link" variant="primary" onClick={createLink} />
          )}
          {invites.length > 0 && (
            <VStack gap={2}>
              <Heading level={3}>Active links</Heading>
              <List density="compact" hasDividers>
                {invites.map((invite) => (
                  <ListItem
                    key={invite.id}
                    label={`${invite.role === "editor" ? "Can edit" : "Can view"}${
                      invite.singleUse ? ", single use" : ""
                    }`}
                    description={
                      <Text type="supporting" color="secondary">
                        {formatTime(invite.$createdAt)}
                      </Text>
                    }
                    endContent={
                      <HStack gap={1}>
                        <Button
                          label="Copy"
                          size="sm"
                          variant="ghost"
                          clickAction={() =>
                            copy(
                              inviteLinkFor(window.location.origin, {
                                canvasId,
                                token: invite.token,
                              }),
                            )
                          }
                        />
                        <Button
                          label="Revoke"
                          size="sm"
                          variant="secondary"
                          onClick={() => {
                            db.delete(app.canvasInvites, invite.id);
                            if (link?.endsWith(invite.token)) setLink(null);
                          }}
                        />
                      </HStack>
                    }
                  />
                ))}
              </List>
            </VStack>
          )}
        </VStack>
      </Dialog>
    </>
  );
}

function formatTime(value: unknown) {
  const date = value instanceof Date ? value : new Date(Number(value));
  if (Number.isNaN(date.getTime())) return "";
  return `Created ${date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })}`;
}
