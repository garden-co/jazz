import { useEffect, useMemo, useState } from "react";
import { useQuery } from "./data/live";
import { AppShell, Button, Center, EmptyState, Spinner } from "@astryxdesign/core";
import { queries } from "./data/queries";
import type { Profile } from "../shared/types";
import { JoinRoom } from "./components/JoinRoom";
import { NewRoomDialog } from "./components/NewRoomDialog";
import { ProfileDialog, ProfileSetup } from "./components/ProfileDialog";
import { RoomNav, type RoomSummary } from "./components/RoomNav";
import { RoomView, usePrefetchRoom } from "./components/RoomView";
import { UnsentMessagesProvider, UnsentNotices } from "./components/UnsentNotices";
import { ProfileDirectoryProvider } from "./lib/profiles";
import { useSearchParam } from "./lib/url-state";

export interface BandChatProps {
  /** Pre-fills the display name on first run (e.g. the Better Auth user name). */
  defaultDisplayName?: string;
  onSignOut?: () => void;
}

/** Rendered inside the Zero provider for the signed-in Better Auth user. */
export function BandChat({ author, ...props }: BandChatProps & { author: string }) {
  return <Workspace author={author} {...props} />;
}

function Loading({ label }: { label: string }) {
  return (
    <Center axis="both" padding={4} className="page-fill">
      <Spinner label={label} />
    </Center>
  );
}

function Workspace({ author, ...props }: BandChatProps & { author: string }) {
  const [myProfiles, myProfilesResult] = useQuery(queries.profiles.mine());
  // None of these depend on the profile: open them together with it, and the
  // open room's messages too when the URL names the room, instead of one
  // after the other as each component mounts.
  const roomData = useRoomData();
  const [selectedRoomId] = useSearchParam("room");
  usePrefetchRoom(selectedRoomId);
  const profile = myProfiles[0];
  if (!profile && myProfilesResult.type !== "complete") return <Loading label="Loading your profile…" />;
  // Profile creation is an explicit first-run action, never a read side effect.
  // Two tabs finishing setup at once can create two profiles; the oldest wins.
  if (!profile)
    return <ProfileSetup author={author} defaultDisplayName={props.defaultDisplayName} />;
  return (
    <ProfileDirectoryProvider me={profile}>
      <UnsentMessagesProvider>
        <Rooms author={author} profile={profile} roomData={roomData} {...props} />
      </UnsentMessagesProvider>
    </ProfileDirectoryProvider>
  );
}

type RoomData = ReturnType<typeof useRoomData>;

/** The rooms, memberships and read markers behind the room list; undefined while loading. */
function useRoomData() {
  const [rooms, roomsResult] = useQuery(queries.rooms.mine());
  const [memberships, membershipsResult] = useQuery(queries.roomMembers.mine());
  const [markers, markersResult] = useQuery(queries.readMarkers.mine());
  const loaded = <T,>(rows: T, result: { type: string }) => (result.type === "unknown" ? undefined : rows);
  return {
    rooms: loaded(rooms, roomsResult),
    memberships: loaded(memberships, membershipsResult),
    markers: loaded(markers, markersResult),
  };
}

function Rooms({
  author,
  profile,
  roomData,
  onSignOut,
}: BandChatProps & { author: string; profile: Profile; roomData: RoomData }) {
  const { rooms = [], memberships = [], markers = [] } = roomData;
  // "No rooms yet" and the join prompt wait for the real answer.
  const isLoadingRooms = !roomData.rooms || !roomData.memberships || !roomData.markers;
  const [selectedRoomId, setSelectedRoomId] = useSearchParam("room");
  const [joinRoomId, setJoinRoomId] = useSearchParam("join");
  const [isNewRoomOpen, setNewRoomOpen] = useState(false);
  const [isProfileOpen, setProfileOpen] = useState(false);
  const [isNavOpen, setNavOpen] = useState(false);

  const summaries = useMemo<RoomSummary[]>(() => {
    const memberOf = new Set(memberships.map((membership) => membership.roomId));
    const lastRead = new Map<string, number>();
    for (const marker of markers) {
      const previous = lastRead.get(marker.roomId);
      if (!previous || previous < marker.lastReadAt) lastRead.set(marker.roomId, marker.lastReadAt);
    }
    return rooms
      .filter((room) => memberOf.has(room.id) || room.createdBy === author)
      .map((room) => {
        const activityAt = new Date(room.lastActivityAt ?? room.createdAt);
        const readAt = lastRead.get(room.id);
        return {
          room,
          activityAt,
          readAt: readAt === undefined ? undefined : new Date(readAt),
          isCreator: room.createdBy === author,
          hasUnread: !!room.lastActivityAt && (!readAt || readAt < room.lastActivityAt),
        };
      })
      .sort((a, b) => b.activityAt.getTime() - a.activityAt.getTime());
  }, [rooms, memberships, markers, author]);

  const selected =
    summaries.find((summary) => summary.room.id === selectedRoomId) ??
    (joinRoomId ? undefined : summaries[0]);

  // A room link for a room you already belong to simply opens it.
  const joinedRoom = joinRoomId
    ? summaries.find((summary) => summary.room.id === joinRoomId)
    : undefined;
  useEffect(() => {
    if (!joinedRoom) return;
    setSelectedRoomId(joinedRoom.room.id);
    setJoinRoomId(null);
  }, [joinedRoom, setSelectedRoomId, setJoinRoomId]);

  function selectRoom(roomId: string) {
    setJoinRoomId(null);
    setSelectedRoomId(roomId);
    setNavOpen(false);
  }

  let main;
  if (isLoadingRooms && !selected) {
    main = <Loading label="Loading rooms…" />;
  } else if (joinRoomId && !joinedRoom) {
    main = <JoinRoom roomId={joinRoomId} profile={profile} onDismiss={() => setJoinRoomId(null)} />;
  } else if (selected) {
    main = <RoomView key={selected.room.id} summary={selected} author={author} />;
  } else {
    main = (
      <Center axis="both" padding={4} className="page-fill">
        <EmptyState
          headingLevel={1}
          title="No rooms yet"
          description="Create a room for your band, then share its link so bandmates can ask to join."
          actions={
            <Button variant="primary" label="Create a room" onClick={() => setNewRoomOpen(true)} />
          }
        />
      </Center>
    );
  }

  return (
    <>
      <AppShell
        variant="section"
        contentPadding={0}
        mobileNav={{ isOpen: isNavOpen, onOpenChange: setNavOpen }}
        sideNav={
          <RoomNav
            rooms={summaries}
            selectedRoomId={selected?.room.id ?? null}
            onSelect={selectRoom}
            onNewRoom={() => setNewRoomOpen(true)}
            onEditProfile={() => setProfileOpen(true)}
            onSignOut={onSignOut}
          />
        }
      >
        <UnsentNotices
          isMember={(roomId) => summaries.some((summary) => summary.room.id === roomId)}
        />
        {main}
      </AppShell>
      <NewRoomDialog
        isOpen={isNewRoomOpen}
        onOpenChange={setNewRoomOpen}
        author={author}
        profile={profile}
        onCreated={selectRoom}
      />
      <ProfileDialog isOpen={isProfileOpen} onOpenChange={setProfileOpen} profile={profile} />
    </>
  );
}
