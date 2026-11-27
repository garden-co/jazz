import { useState, type FormEvent } from "react";
import { useDb } from "../data/live";
import {
  Banner,
  Button,
  Dialog,
  DialogHeader,
  HStack,
  TextInput,
  VStack,
} from "@astryxdesign/core";
import { mutators } from "../data/mutators";
import type { Profile } from "../../shared/types";

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
  const db = useDb();
  const [name, setName] = useState("");
  const [error, setError] = useState<string | null>(null);

  function create(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setError(null);
    // The room is usable before the server confirms it. The room and its
    // creator's membership are one mutation. The dialog closes and selects
    // the room in this handler, before the new room renders.
    const roomId = crypto.randomUUID();
    const committed = db.mutate(
      mutators.rooms.create({ id: roomId, memberId: crypto.randomUUID(), name: trimmed, profileId: profile.id }),
    );
    setName("");
    onOpenChange(false);
    onCreated(roomId);
    // If the server rejects the room, it disappears; reopen the dialog with
    // the name and the reason so the creator can see what happened.
    void committed.server.then((result) => {
      if (result.type !== "error") return;
      setName(trimmed);
      setError(`Could not create the room: ${result.error.message}`);
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
