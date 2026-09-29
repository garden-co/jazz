"use client";

import { useState } from "react";
import { useAll } from "jazz-tools/react";
import { Button } from "@astryxdesign/core/Button";
import { EmptyState } from "@astryxdesign/core/EmptyState";
import { Heading } from "@astryxdesign/core/Heading";
import { Icon } from "@astryxdesign/core/Icon";
import { List, ListItem } from "@astryxdesign/core/List";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { app } from "../schema";
import { useAccountId, useStore } from "./library-data";

/** Pending invitations addressed to this account, readable before acceptance. */
export function usePendingInvitations() {
  const me = useAccountId();
  return useAll(
    me
      ? app.invitations.where({ subject: me, status: "pending" }).select("playlist_id", "role")
      : undefined,
  );
}

export function Invitations({ onAccepted }: { onAccepted(): void }) {
  const me = useAccountId();
  const store = useStore();
  const pending = usePendingInvitations();
  const [copied, setCopied] = useState(false);
  const [accepting, setAccepting] = useState<string>();

  async function accept(id: string) {
    setAccepting(id);
    try {
      await store.acceptInvitation(id);
      onAccepted();
    } finally {
      setAccepting(undefined);
    }
  }

  return (
    <VStack gap={6} maxWidth={720}>
      <VStack gap={2}>
        <Heading level={2}>Your account ID</Heading>
        <Text color="secondary">
          Playlist owners invite you by this ID. It is your Jazz account, not your email.
        </Text>
        <HStack gap={2} vAlign="center" wrap="wrap">
          <code className="rp-account-id">{me ?? "Not connected"}</code>
          <Button
            label={copied ? "Copied" : "Copy"}
            variant="secondary"
            size="sm"
            icon={<Icon icon="copy" size="sm" />}
            isDisabled={!me}
            onClick={() => {
              if (!me) return;
              void navigator.clipboard.writeText(me).then(() => setCopied(true));
            }}
          />
        </HStack>
      </VStack>
      <VStack gap={2}>
        <Heading level={2}>Invitations</Heading>
        {pending.data && pending.data.length > 0 ? (
          <List hasDividers>
            {pending.data.map((invitation) => (
              <ListItem
                key={invitation.id}
                label={invitation.role === "editor" ? "Edit a playlist" : "Listen to a playlist"}
                description="The playlist's name and tracks become visible once you accept."
                endContent={
                  <Button
                    label="Accept"
                    variant="primary"
                    size="sm"
                    isLoading={accepting === invitation.id}
                    onClick={() => void accept(invitation.id)}
                  />
                }
              />
            ))}
          </List>
        ) : (
          <EmptyState
            isCompact
            title="No pending invitations"
            description="When someone shares a playlist with you, it appears here."
          />
        )}
      </VStack>
    </VStack>
  );
}
