import { useCallback, useEffect, useMemo, useState } from "react";
import { JazzProvider, useAll, useJazzSession } from "jazz-tools/react";
import { AppShell, Button, Center, EmptyState, Spinner } from "@astryxdesign/core";
import { app, type Profile } from "../jazz.generated";
import { JoinRoom } from "./components/JoinRoom";
import { NewRoomDialog } from "./components/NewRoomDialog";
import { ProfileDialog, ProfileSetup } from "./components/ProfileDialog";
import { RoomActivityProbes } from "./components/RoomActivity";
import { RoomNav } from "./components/RoomNav";
import { RoomView, usePrefetchRoom } from "./components/RoomView";
import { UnsentMessagesProvider, UnsentNotices } from "./components/UnsentNotices";
import { ProfileDirectoryProvider } from "./lib/profiles";
import { summariseRooms, type NewestMessage } from "./lib/room-summaries";
import { useSearchParam } from "./lib/url-state";

export interface BandChatProps {
  /** Pre-fills the display name on first run (e.g. the Better Auth user name). */
  defaultDisplayName?: string;
  onSignOut?: () => void;
}

/** Rendered inside the Better Auth provider. */
export function BandChat(props: BandChatProps) {
  const session = useJazzSession();
  const author = session.user?.accountId;
  return author ? <Workspace author={author} {...props} /> : <Loading label="Loading identity…" />;
}

function Loading({ label }: { label: string }) {
  return (
    <Center axis="both" padding={4} className="page-fill">
      <Spinner label={label} />
    </Center>
  );
}

function Workspace({ author, ...props }: BandChatProps & { author: string }) {
  const { data: myProfiles } = useAll(
    app.profiles.where({ author }).orderBy("$createdAt", "asc").limit(1),
  );
  // None of these depend on the profile: open them together with it, and the
  // open room's messages too when the URL names the room, instead of one
  // after the other as each component mounts.
  const roomData = useRoomData(author);
  const [selectedRoomId] = useSearchParam("room");
  usePrefetchRoom(selectedRoomId);
  if (!myProfiles) return <Loading label="Loading your profile…" />;
  // Profile creation is an explicit first-run action, never a read side effect.
  // Two tabs finishing setup at once can create two profiles; the oldest wins.
  const profile = myProfiles[0];
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

/**
 * The rooms this account is sent (it holds a member row of each) and its
 * read markers; each undefined while loading. The list never waits for the
 * markers: they need the local-first client and a key.
 */
function useRoomData(author: string) {
  const { data: rooms } = useAll(app.rooms.select("*", "$createdBy", "$createdAt"));
  const { data: markers } = useAll(app.readMarkers.where({ reader: author }));
  return { rooms, markers };
}

function Rooms({
  author,
  profile,
  roomData,
  onSignOut,
}: BandChatProps & { author: string; profile: Profile; roomData: RoomData }) {
  const { rooms = [] } = roomData;
  // "No rooms yet" and the join prompt wait for the real answer.
  const isLoadingRooms = !roomData.rooms;
  const [selectedRoomId, setSelectedRoomId] = useSearchParam("room");
  const [joinRoomId, setJoinRoomId] = useSearchParam("join");
  const [isNewRoomOpen, setNewRoomOpen] = useState(false);
  const [isProfileOpen, setProfileOpen] = useState(false);
  const [isNavOpen, setNavOpen] = useState(false);
  const [newest, setNewest] = useState<ReadonlyMap<string, NewestMessage | null>>(new Map());
  const onNewest = useCallback((roomId: string, latest: NewestMessage | null | undefined) => {
    setNewest((current) => {
      if (latest === undefined) {
        if (!current.has(roomId)) return current;
        const next = new Map(current);
        next.delete(roomId);
        return next;
      }
      const next = new Map(current);
      next.set(roomId, latest);
      return next;
    });
  }, []);

  const markers = roomData.markers;
  const roomIds = useMemo(() => rooms.map((room) => room.id), [rooms]);
  const summaries = useMemo(() => {
    let lastRead: Map<string, Date> | undefined;
    if (markers) {
      lastRead = new Map();
      for (const marker of markers) {
        const previous = lastRead.get(marker.roomId);
        if (!previous || previous < marker.lastReadAt)
          lastRead.set(marker.roomId, marker.lastReadAt);
      }
    }
    return summariseRooms({ rooms, newest, lastRead, author, profileId: profile.id });
  }, [rooms, newest, markers, author, profile.id]);

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
      <RoomActivityProbes roomIds={roomIds} onNewest={onNewest} />
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
