"use client";

import { useAll } from "jazz-tools/react";
import {
  Badge,
  HStack,
  Button,
  MoreMenu,
  SideNav,
  SideNavHeading,
  SideNavItem,
  SideNavSection,
  Text,
  Timestamp,
} from "@astryxdesign/core";
import { app, type Room } from "../../schema";
import { ProfileAvatar, useDirectory } from "../lib/profiles";

export interface RoomSummary {
  room: Room & { $createdAt: Date; $createdBy: { account: string } };
  activityAt: Date;
  readAt: Date | undefined;
  isCreator: boolean;
  hasUnread: boolean;
}

// Counting stops here; the badge then reads "99+".
const UNREAD_CAP = 100;

export function RoomNav({
  rooms,
  selectedRoomId,
  onSelect,
  onNewRoom,
  onEditProfile,
  onSignOut,
}: {
  rooms: RoomSummary[];
  selectedRoomId: string | null;
  onSelect: (roomId: string) => void;
  onNewRoom: () => void;
  onEditProfile: () => void;
  onSignOut?: () => void;
}) {
  const { me } = useDirectory();
  return (
    <SideNav
      header={
        <SideNavHeading
          heading="BandChat"
          headerEndContent={<Button label="New room" size="sm" onClick={onNewRoom} />}
        />
      }
      footer={
        <HStack gap={2} vAlign="center" paddingInline={2} paddingBlock={2}>
          <ProfileAvatar profile={me} size="sm" />
          <Text maxLines={1} weight="semibold">
            {me.displayName}
          </Text>
          <MoreMenu
            label="Account"
            size="sm"
            placement="above"
            items={[
              { label: "Edit profile", onClick: onEditProfile },
              ...(onSignOut ? [{ label: "Sign out", onClick: onSignOut }] : []),
            ]}
          />
        </HStack>
      }
    >
      <SideNavSection title="Rooms">
        {rooms.map((summary) => (
          <RoomNavItem
            key={summary.room.id}
            summary={summary}
            isSelected={summary.room.id === selectedRoomId}
            onSelect={onSelect}
          />
        ))}
      </SideNavSection>
    </SideNav>
  );
}

function RoomNavItem({
  summary,
  isSelected,
  onSelect,
}: {
  summary: RoomSummary;
  isSelected: boolean;
  onSelect: (roomId: string) => void;
}) {
  const { me } = useDirectory();
  const { room, readAt, hasUnread } = summary;
  // Only rooms with activity newer than this reader's marker pay for a count.
  const { data: unread = [] } = useAll(
    hasUnread && !isSelected
      ? app.messages
          .where(
            readAt
              ? { roomId: room.id, senderId: { ne: me.id }, $createdAt: { gt: readAt } }
              : { roomId: room.id, senderId: { ne: me.id } },
          )
          .select("id")
          .limit(UNREAD_CAP)
      : undefined,
  );
  const count = unread.length;
  return (
    <SideNavItem
      label={room.name}
      isSelected={isSelected}
      onClick={() => onSelect(room.id)}
      endContent={
        count > 0 ? (
          <Badge
            variant="info"
            label={count >= UNREAD_CAP ? "99+" : String(count)}
            aria-label={`${count} unread`}
          />
        ) : (
          <Timestamp
            value={summary.activityAt.toISOString()}
            format="relative_short"
            hasTooltip={false}
          />
        )
      }
    />
  );
}
