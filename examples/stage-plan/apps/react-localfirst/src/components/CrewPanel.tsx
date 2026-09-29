import { useState } from "react";
import { Copy, RefreshCw, UserMinus } from "lucide-react";
import { useAll, useDb } from "jazz-tools/react";
import { Avatar } from "@astryxdesign/core/Avatar";
import { Badge } from "@astryxdesign/core/Badge";
import { Button } from "@astryxdesign/core/Button";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { List, ListItem } from "@astryxdesign/core/List";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";
import { TextInput } from "@astryxdesign/core/TextInput";
import { app, type Show } from "../../schema.js";
import { rotateInvite, type CrewMember } from "../data/actions.js";
import { useMe } from "../data/me.js";
import { href, navigate } from "../router.js";

export function CrewPanel({ show, crew }: { show: Show; crew: CrewMember[] }) {
  const db = useDb();
  const me = useMe();
  const isChief = show.chiefAccount === me.account;
  const myMembership = crew.find((member) => member.account === me.account);

  return (
    <VStack gap={6}>
      {isChief && <InviteLink show={show} />}
      <VStack gap={2}>
        <Heading level={2}>People on this show</Heading>
        <List hasDividers>
          {crew.map((member) => {
            const name = member.crew?.name ?? "Crew member";
            const isMe = member.account === me.account;
            return (
              <ListItem
                key={member.id}
                startContent={<Avatar name={name} size="sm" />}
                label={isMe ? `${name} (you)` : name}
                endContent={
                  <HStack gap={2} vAlign="center">
                    <Badge
                      label={member.role === "chief" ? "Crew chief" : "Crew"}
                      variant={member.role === "chief" ? "info" : "neutral"}
                    />
                    {isChief && !isMe && (
                      <IconButton
                        label={`Remove ${name} from the crew`}
                        variant="ghost"
                        size="sm"
                        icon={<Icon icon={UserMinus} size="sm" />}
                        onClick={() => db.delete(app.showCrew, member.id)}
                      />
                    )}
                  </HStack>
                }
              />
            );
          })}
        </List>
      </VStack>
      {!isChief && myMembership && (
        <HStack>
          <Button
            label="Leave this show"
            variant="destructive"
            onClick={() => {
              db.delete(app.showCrew, myMembership.id);
              navigate(href.shows());
            }}
          />
        </HStack>
      )}
    </VStack>
  );
}

/** Only the chief can read the show's invite code, so only they see this. */
function InviteLink({ show }: { show: Show }) {
  const db = useDb();
  const [copied, setCopied] = useState(false);
  const { data: invites = [] } = useAll(app.showInvites.where({ showId: show.id }));
  const invite = invites[0];
  const link = invite
    ? `${window.location.origin}${window.location.pathname}${href.join(show.id, invite.code)}`
    : "";

  return (
    <VStack gap={2}>
      <Heading level={2}>Invite crew</Heading>
      <Text color="secondary">
        Anyone with this link can join the crew and edit the board. Make a new link to stop the old
        one from working.
      </Text>
      <HStack gap={2} vAlign="end" wrap="wrap">
        <TextInput label="Invite link" isLabelHidden value={link} isReadOnly width="100%" />
        <HStack gap={2}>
          <Button
            label={copied ? "Copied" : "Copy link"}
            icon={<Icon icon={Copy} size="sm" />}
            isDisabled={!link}
            clickAction={async () => {
              await navigator.clipboard.writeText(link);
              setCopied(true);
            }}
          />
          <Button
            label="New link"
            variant="ghost"
            icon={<Icon icon={RefreshCw} size="sm" />}
            onClick={() => {
              setCopied(false);
              void rotateInvite(
                db,
                show.id,
                invites.map((old) => old.id),
              );
            }}
          />
        </HStack>
      </HStack>
    </VStack>
  );
}
