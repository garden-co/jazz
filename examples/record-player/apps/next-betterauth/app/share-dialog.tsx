"use client";

import { useState } from "react";
import { useAll } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Banner } from "@astryxdesign/core/Banner";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Layout, LayoutContent } from "@astryxdesign/core/Layout";
import { List, ListItem } from "@astryxdesign/core/List";
import { SegmentedControl, SegmentedControlItem } from "@astryxdesign/core/SegmentedControl";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app } from "../schema";
import type { InvitationRole } from "../src/record-player";
import { shortId } from "./format";
import { useStore, type PlaylistSummary } from "./library-data";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Owner-only sharing. An invitation names the recipient's Jazz account ID and
 * a role; the recipient must accept it before the playlist becomes readable
 * (listener) or editable (editor). Revoking withdraws that access.
 */
export function ShareDialog({
  playlist,
  isOpen,
  onClose,
}: {
  playlist: PlaylistSummary;
  isOpen: boolean;
  onClose(): void;
}) {
  const store = useStore();
  const [recipient, setRecipient] = useState("");
  const [role, setRole] = useState<InvitationRole>("listener");
  const [error, setError] = useState<string>();
  const invitations = useAll(
    isOpen
      ? app.invitations.where({ playlist_id: playlist.id }).select("subject", "role", "status")
      : undefined,
  );

  async function invite() {
    const subject = recipient.trim();
    if (!UUID.test(subject)) {
      setError("Paste the account ID from the other person's Invitations tab.");
      return;
    }
    setError(undefined);
    await store.invite(playlist.id, subject, role);
    setRecipient("");
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={(open) => !open && onClose()} purpose="info" width={560}>
      <Layout
        height="auto"
        header={
          <DialogHeader
            title={`Share ${playlist.name}`}
            subtitle="Listeners can play it. Editors can also add, remove and reorder tracks."
            onOpenChange={(open) => !open && onClose()}
          />
        }
        content={
          <LayoutContent>
            <VStack gap={4}>
              <TextInput
                label="Account ID"
                description="The other person finds theirs on their Invitations tab."
                placeholder="00000000-0000-0000-0000-000000000000"
                value={recipient}
                onChange={setRecipient}
                onEnter={() => void invite()}
                status={error ? { type: "error", message: error } : undefined}
              />
              <HStack gap={2} vAlign="center" justify="between" wrap="wrap">
                <SegmentedControl
                  label="Role"
                  value={role}
                  onChange={(value) => setRole(value as InvitationRole)}
                >
                  <SegmentedControlItem value="listener" label="Listener" />
                  <SegmentedControlItem value="editor" label="Editor" />
                </SegmentedControl>
                <Button
                  label="Send invitation"
                  variant="primary"
                  isDisabled={!recipient.trim()}
                  onClick={() => void invite()}
                />
              </HStack>
              {invitations.data && invitations.data.length > 0 ? (
                <List density="compact" hasDividers header={<Text type="label">Invited</Text>}>
                  {invitations.data.map((invitation) => (
                    <ListItem
                      key={invitation.id}
                      label={<Text hasTabularNumbers>{shortId(invitation.subject)}…</Text>}
                      description={invitation.role === "editor" ? "Editor" : "Listener"}
                      endContent={
                        <HStack gap={2} vAlign="center">
                          <Badge
                            variant={
                              invitation.status === "accepted"
                                ? "success"
                                : invitation.status === "pending"
                                  ? "warning"
                                  : "neutral"
                            }
                            label={
                              invitation.status === "accepted"
                                ? "Accepted"
                                : invitation.status === "pending"
                                  ? "Pending"
                                  : "Revoked"
                            }
                          />
                          {invitation.status !== "revoked" && (
                            <Button
                              label="Revoke"
                              variant="ghost"
                              size="sm"
                              onClick={() => store.revokeInvitation(invitation.id)}
                            />
                          )}
                        </HStack>
                      }
                    />
                  ))}
                </List>
              ) : (
                <Banner
                  status="info"
                  title="Not shared yet"
                  description="Invitations appear here with their status."
                />
              )}
            </VStack>
          </LayoutContent>
        }
      />
    </Dialog>
  );
}
