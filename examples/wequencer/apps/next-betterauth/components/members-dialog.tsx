"use client";

import { useState, type FormEvent } from "react";
import { useDb } from "jazz-tools/react";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { Dialog, DialogHeader } from "@astryxdesign/core/Dialog";
import { Divider } from "@astryxdesign/core/Divider";
import { Heading } from "@astryxdesign/core/Heading";
import { HStack } from "@astryxdesign/core/HStack";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { List, ListItem } from "@astryxdesign/core/List";
import { Selector } from "@astryxdesign/core/Selector";
import { Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { VStack } from "@astryxdesign/core/VStack";
import { app, type MemberRole } from "@/schema";
import { AccountId } from "@/components/account-id";
import type { PresenceRow } from "@/components/presence-avatars";
import { ROLE_LABELS } from "@/lib/roles";
import type { ReportWrite } from "@/lib/report-write";

type Member = { id: string; member_author: string; role: MemberRole };

const ROLE_OPTIONS = [
  { value: "editor", label: "Editor", description: "Edits pads, mix and transport" },
  { value: "viewer", label: "Viewer", description: "Listens and watches" },
];

/**
 * Everyone can see who is in the session. Only the session's creator (its
 * immutable `$createdBy`) can add or remove members; see issue #2100 for
 * richer ownership semantics.
 */
export function MembersDialog({
  isOpen,
  onOpenChange,
  sessionId,
  members,
  presence,
  author,
  isCreator,
  reportWrite,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  sessionId: string;
  members: Member[];
  presence: PresenceRow[];
  author: string | undefined;
  isCreator: boolean;
  reportWrite: ReportWrite;
}) {
  const db = useDb();
  const [accountId, setAccountId] = useState("");
  const [role, setRole] = useState<"editor" | "viewer">("editor");

  // Display names are only readable once a member has shown presence here.
  const names = new Map<string, string>();
  for (const row of presence)
    if (row.profile) names.set(row.profile.author, row.profile.displayName);

  function add(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const invited = accountId.trim();
    if (!invited) return;
    void reportWrite(
      db
        .insert(app.session_members, { session_id: sessionId, member_author: invited, role })
        .wait({ tier: "global" }),
      "Adding the member",
    );
    setAccountId("");
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} width={520}>
      <DialogHeader title="Members" onOpenChange={onOpenChange} hasDivider />
      <VStack gap={4} padding={4}>
        <List hasDividers density="compact">
          {members.map((member) => {
            const isYou = member.member_author === author;
            const name = isYou
              ? "You"
              : (names.get(member.member_author) ?? `Account ${member.member_author.slice(0, 8)}`);
            return (
              <ListItem
                key={member.id}
                label={name}
                description={
                  names.has(member.member_author) || isYou
                    ? undefined
                    : "Hasn't opened the session yet"
                }
                endContent={
                  <HStack gap={2} align="center">
                    <Badge
                      label={ROLE_LABELS[member.role]}
                      variant={member.role === "viewer" ? "neutral" : "info"}
                    />
                    {isCreator && !isYou ? (
                      <IconButton
                        variant="ghost"
                        size="sm"
                        label={`Remove ${name}`}
                        tooltip={`Remove ${name}`}
                        icon={<Icon icon="close" size="sm" />}
                        onClick={() =>
                          void reportWrite(
                            db.delete(app.session_members, member.id).wait({ tier: "global" }),
                            "Removing the member",
                          )
                        }
                      />
                    ) : null}
                  </HStack>
                }
              />
            );
          })}
        </List>
        {isCreator ? (
          <>
            <Divider />
            <form onSubmit={add}>
              <VStack gap={3}>
                <Heading level={3}>Add a bandmate</Heading>
                <TextInput
                  label="Collaborator account ID"
                  description="Your bandmate finds it on their sessions page."
                  value={accountId}
                  onChange={setAccountId}
                  isRequired
                />
                <Selector
                  label="Role"
                  options={ROLE_OPTIONS}
                  value={role}
                  onChange={(value) => setRole(value as "editor" | "viewer")}
                />
                <HStack justify="end">
                  <Button type="submit" variant="primary" label="Add collaborator" />
                </HStack>
              </VStack>
            </form>
          </>
        ) : (
          <Text type="supporting">Only the session creator can add or remove members.</Text>
        )}
        {author ? (
          <>
            <Divider />
            <AccountId accountId={author} />
          </>
        ) : null}
      </VStack>
    </Dialog>
  );
}
