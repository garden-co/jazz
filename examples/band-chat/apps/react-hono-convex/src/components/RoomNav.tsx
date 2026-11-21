import { useQuery } from "convex/react";
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
import { api } from "@/convex/_generated/api";
import type { Room } from "../lib/model";
import { ProfileAvatar, useDirectory } from "../lib/profiles";

export interface RoomSummary {
  room: Room;
  activityAt: Date;
  readAt: Date | undefined;
  isCreator: boolean;
  hasUnread: boolean;
}

// Counting stops here (in messages.unreadCounts); the badge then reads "99+".
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
  // Only rooms with activity newer than this reader's marker pay for a count.
  const counted = rooms
    .filter((summary) => summary.hasUnread && summary.room._id !== selectedRoomId)
    .map((summary) => ({ roomId: summary.room._id, since: summary.readAt?.getTime() }));
  const unread = useQuery(api.messages.unreadCounts, counted.length ? { rooms: counted } : "skip");
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
            key={summary.room._id}
            summary={summary}
            isSelected={summary.room._id === selectedRoomId}
            count={summary.room._id === selectedRoomId ? 0 : (unread?.[summary.room._id] ?? 0)}
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
  count,
  onSelect,
}: {
  summary: RoomSummary;
  isSelected: boolean;
  count: number;
  onSelect: (roomId: string) => void;
}) {
  const { room } = summary;
  return (
    <SideNavItem
      label={room.name}
      isSelected={isSelected}
      onClick={() => onSelect(room._id)}
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
