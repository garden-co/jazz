import { useState, type FormEvent } from "react";
import { id } from "@instantdb/react";
import {
  Banner,
  Button,
  Dialog,
  DialogHeader,
  HStack,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { db, transact, type Profile } from "../lib/db";

export function NewRoomDialog({
  isOpen,
  onOpenChange,
  author,
  profile,
  onCreated,
}: {
  isOpen: boolean;
  onOpenChange: (isOpen: boolean) => void;
  author: string;
  profile: Profile;
  onCreated: (roomId: string) => void;
}) {
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setError(null);
    // Optimistic: the room is usable before the server confirms it. The
    // room and its creator's membership commit together; the membership's
    // rule sees the room created in the same transaction. The dialog closes
    // and selects the room in this handler, before the new room renders, or
    // it would render under the still-open modal.
    const roomId = id();
    const committed = transact([
      db.tx.rooms[roomId].create({ name: trimmed, creator: author, createdAt: Date.now() }),
      db.tx.roomMembers[id()]
        .create({ memberAuthor: author })
        .link({ room: roomId, memberProfile: profile.id }),
    ]);
    setName("");
    onOpenChange(false);
    onCreated(roomId);
    // If the server rejects the room, it disappears; reopen the dialog with
    // the name and the reason so the creator can see what happened.
    committed.catch((cause: unknown) => {
      setName(trimmed);
      setError(
        `Could not create the room: ${cause instanceof Error ? cause.message : String(cause)}`,
      );
      onOpenChange(true);
    });
  }

  return (
    <Dialog isOpen={isOpen} onOpenChange={onOpenChange} purpose="form">
      <DialogHeader title="New room" onOpenChange={onOpenChange} />
      <form onSubmit={create}>
        <VStack gap={4} padding={4}>
          <TextInput
            label="Room name"
            placeholder="Rehearsal"
            value={name}
            onChange={setName}
            hasAutoFocus
            isRequired
          />
          {error ? (
            <Banner status="error" title={error} container="section" collapsible={false} />
          ) : null}
          <HStack gap={2} justify="end">
            <Button label="Cancel" variant="ghost" onClick={() => onOpenChange(false)} />
            <Button label="Create room" variant="primary" type="submit" />
          </HStack>
        </VStack>
      </form>
    </Dialog>
  );
}
