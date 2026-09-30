"use client";

import { useEffect, useMemo, useState } from "react";
import type { DbConfig } from "jazz-tools";
import { JazzProvider, useAll, useSession } from "jazz-tools/react";
import { AppShell, Button, Center, EmptyState, Spinner } from "@astryxdesign/core";
import { app, type Profile } from "../schema";
import { ThemeProvider } from "../components/theme-provider";
import { JoinRoom } from "./components/JoinRoom";
import { NewRoomDialog } from "./components/NewRoomDialog";
import { ProfileDialog, ProfileSetup } from "./components/ProfileDialog";
import { RoomNav, type RoomSummary } from "./components/RoomNav";
import { RoomView } from "./components/RoomView";
import { ProfileDirectoryProvider } from "./lib/profiles";
import { memoryStore, ParamStoreProvider, useSearchParam } from "./lib/url-state";

export interface BandChatProps {
  /** Pre-fills the display name on first run (e.g. the Better Auth user name). */
  defaultDisplayName?: string;
  onSignOut?: () => void;
}

/** Rendered inside the external-auth provider in the Next dashboard. */
export function BandChat(props: BandChatProps) {
  const session = useSession();
  const author = session?.user.account;
  return author ? <Workspace author={author} {...props} /> : <Loading label="Loading identity…" />;
}

/** Browser receipt entrypoint. The production dashboard never uses local-first auth here. */
export function BandChatPreview({
  config,
  initialParams,
}: {
  config: DbConfig;
  /** Query-string state for this preview, e.g. `{ join: roomId }` for a room link. */
  initialParams?: Record<string, string>;
}) {
  const [store] = useState(() => memoryStore(initialParams));
  return (
    <ThemeProvider>
      <ParamStoreProvider store={store}>
        <JazzProvider config={config} fallback={<Loading label="Opening local stage…" />}>
          <BandChat />
        </JazzProvider>
      </ParamStoreProvider>
    </ThemeProvider>
  );
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
  if (!myProfiles) return <Loading label="Loading your profile…" />;
  // Profile creation is an explicit first-run action, never a read side effect.
  // Two tabs finishing setup at once can create two profiles; the oldest wins.
  const profile = myProfiles[0];
  if (!profile)
    return <ProfileSetup author={author} defaultDisplayName={props.defaultDisplayName} />;
  return (
    <ProfileDirectoryProvider me={profile}>
      <Rooms author={author} profile={profile} {...props} />
    </ProfileDirectoryProvider>
  );
}

function Rooms({
  author,
  profile,
  onSignOut,
}: BandChatProps & { author: string; profile: Profile }) {
  const { data: rooms = [] } = useAll(app.rooms.select("*", "$createdBy", "$createdAt"));
  const { data: memberships = [] } = useAll(app.roomMembers.where({ memberAuthor: author }));
  const { data: markers = [] } = useAll(app.readMarkers.where({ reader: author }));
  const [selectedRoomId, setSelectedRoomId] = useSearchParam("room");
  const [joinRoomId, setJoinRoomId] = useSearchParam("join");
  const [isNewRoomOpen, setNewRoomOpen] = useState(false);
  const [isProfileOpen, setProfileOpen] = useState(false);
  const [isNavOpen, setNavOpen] = useState(false);

  const summaries = useMemo<RoomSummary[]>(() => {
    const memberOf = new Set(memberships.map((membership) => membership.roomId));
    const lastRead = new Map<string, Date>();
    for (const marker of markers) {
      const previous = lastRead.get(marker.roomId);
      if (!previous || previous < marker.lastReadAt) lastRead.set(marker.roomId, marker.lastReadAt);
    }
    return rooms
      .filter((room) => memberOf.has(room.id) || room.$createdBy.account === author)
      .map((room) => {
        const activityAt = room.lastActivityAt ?? room.$createdAt;
        const readAt = lastRead.get(room.id);
        return {
          room,
          activityAt,
          readAt,
          isCreator: room.$createdBy.account === author,
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
  if (joinRoomId && !joinedRoom) {
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
